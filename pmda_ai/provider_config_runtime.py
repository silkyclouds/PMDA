"""Runtime-owned AI provider configuration and model probe helpers."""

from __future__ import annotations

from typing import Any

_RUNTIME: Any | None = None

_EXTRACTED_NAMES = {
    '_reinit_ai_from_globals',
    '_reload_ai_config_and_reinit',
    '_wait_for_codex_runtime_ready_for_scan',
    '_probe_model',
    '_probe_ai_choose_best_response',
    '_openai_request_timeout_seconds',
    '_normalize_provider_id',
    '_provider_auth_mode',
    '_openai_error_allows_codex_fallback',
    '_ai_context_from_analysis_type',
    '_get_ai_provider_preferences',
    '_save_ai_provider_preferences',
    '_openai_api_key_mode_enabled',
    '_openai_codex_oauth_mode_enabled',
    '_provider_mode_enabled',
    '_provider_mode_disabled_reason',
    '_openai_api_runtime_available',
    '_openai_codex_runtime_available',
    '_resolve_provider_for_runtime',
    '_resolve_ai_runtime_availability',
    '_resolve_openai_client_for_runtime',
    '_openai_chat_text',
    '_ollama_prewarm_model',
    '_ollama_route_for_analysis',
    'api_openai_check',
    'api_openai_models',
    'api_anthropic_models',
    'api_google_models',
    '_local_network_ipv4_candidates',
    'api_ai_models',
}

_MUTABLE_GLOBAL_NAMES = {
    'openai_client',
    'anthropic_client',
    'google_client',
    'google_client_configured',
    'ollama_url',
    'ai_provider_ready',
    'RESOLVED_MODEL',
    'RESOLVED_PARAM_STYLE',
    'RESOLVED_STOP_OK',
    'AI_FUNCTIONAL_ERROR_MSG',
    'OPENAI_LAST_PROBE_401',
    'OPENAI_MODEL_PROBE_LAST_ERROR',
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
        raise RuntimeError("AI provider config runtime is not bound")
    return _RUNTIME


def _sync_runtime_globals(runtime: Any) -> None:
    for name in _MUTABLE_GLOBAL_NAMES:
        if name in globals():
            try:
                setattr(runtime, name, globals()[name])
            except Exception:
                pass


def _openai_request_timeout_seconds() -> float:
    """Return the effective OpenAI request timeout with runtime override support."""
    mod = _runtime_module()
    base_timeout = float(getattr(mod, "AI_OPENAI_REQUEST_TIMEOUT_SEC", 120.0) or 120.0)
    try:
        val = float(
            getattr(
                mod,
                "AI_OPENAI_REQUEST_TIMEOUT_SEC",
                base_timeout,
            )
            or base_timeout
        )
    except Exception:
        val = base_timeout
    return max(10.0, min(180.0, val))


def _normalize_provider_id(provider_id: str, *, fallback: str = "openai-api") -> str:
    raw = str(provider_id or "").strip().lower()
    if not raw:
        return fallback
    if raw == "openai":
        return "openai-api"
    if raw in {"openai-api", "openai-codex", "anthropic", "google", "ollama"}:
        return raw
    return fallback


def _provider_auth_mode(provider_id: str) -> str:
    pid = _normalize_provider_id(provider_id, fallback="")
    if pid == "openai-codex":
        return "oauth"
    if pid in {"openai-api", "anthropic", "google"}:
        return "api_key"
    if pid == "ollama":
        return "none"
    return "unknown"


def _openai_error_allows_codex_fallback(exc: Any) -> bool:
    text = str(exc or "").strip().lower()
    if not text:
        return False
    markers = (
        "429",
        "401",
        "403",
        "quota",
        "insufficient_quota",
        "rate limit",
        "rate_limit",
        "billing",
        "invalid_api_key",
        "api key",
        "authentication",
        "unauthorized",
        "service unavailable",
        "503",
    )
    return any(marker in text for marker in markers)


def _ai_context_from_analysis_type(analysis_type: str) -> str:
    text = str(analysis_type or "").strip().lower()
    if not text:
        return "batch"
    if any(tok in text for tok in ("assistant", "interactive", "manual", "review")):
        return "interactive"
    if "web" in text and "search" in text:
        return "web_search"
    return "batch"


def _reinit_ai_from_globals():
    """Re-initialize AI clients from current module globals (after settings save). No restart needed."""
    global openai_client, anthropic_client, google_client, google_client_configured, ollama_url, ai_provider_ready
    global RESOLVED_MODEL, RESOLVED_PARAM_STYLE, RESOLVED_STOP_OK, AI_FUNCTIONAL_ERROR_MSG
    mod = _runtime_module()
    provider = (getattr(mod, "AI_PROVIDER", "") or "openai").strip().lower()
    openai_key = getattr(mod, "OPENAI_API_KEY", "") or ""
    anthropic_key = getattr(mod, "ANTHROPIC_API_KEY", "") or ""
    google_key = getattr(mod, "GOOGLE_API_KEY", "") or ""
    ollama_u = (getattr(mod, "OLLAMA_URL", "") or "").strip().rstrip("/")
    openai_model = getattr(mod, "OPENAI_MODEL", "gpt-4") or "gpt-4"
    _merged = getattr(mod, "merged", None) or {}

    openai_client = None
    anthropic_client = None
    google_client = None
    google_client_configured = False
    ollama_url = None
    ai_provider_ready = False

    api_key_mode_enabled = bool(getattr(mod, "OPENAI_ENABLE_API_KEY_MODE", OPENAI_ENABLE_API_KEY_MODE))
    codex_mode_enabled = bool(getattr(mod, "OPENAI_ENABLE_CODEX_OAUTH_MODE", OPENAI_ENABLE_CODEX_OAUTH_MODE))

    if api_key_mode_enabled and openai_key:
        os.environ["OPENAI_API_KEY"] = openai_key
        try:
            openai_client = OpenAI(timeout=_openai_request_timeout_seconds())
            logging.info("OpenAI client re-initialized (settings applied)")
        except Exception as e:
            logging.warning("OpenAI client re-init failed: %s", e)

    if anthropic_key and anthropic:
        try:
            anthropic_client = anthropic.Anthropic(api_key=anthropic_key)
            logging.info("Anthropic client re-initialized (settings applied)")
        except Exception as e:
            logging.warning("Anthropic client re-init failed: %s", e)

    if google_key and genai:
        try:
            google_client = genai.Client(api_key=google_key)
            google_client_configured = True
            logging.info("Google Gemini client re-initialized (settings applied)")
        except Exception as e:
            logging.warning("Google client re-init failed: %s", e)

    if ollama_u:
        try:
            r = requests.get(f"{ollama_u}/api/tags", timeout=5)
            if r.status_code == 200:
                ollama_url = ollama_u
                logging.info("Ollama re-verified at %s (settings applied)", ollama_url)
            else:
                logging.warning("Ollama not accessible at %s (HTTP %d)", ollama_u, r.status_code)
        except Exception as e:
            logging.warning("Ollama re-check failed: %s", e)

    if provider in {"openai", "openai-api", "openai-codex"}:
        if provider == "openai-codex":
            codex_ok, codex_reason = _openai_codex_token_health(_current_user_id_or_zero(), force_refresh=False)
            if codex_mode_enabled and codex_ok:
                ai_provider_ready = True
                RESOLVED_MODEL = (openai_model or "").strip() or "codex"
                RESOLVED_PARAM_STYLE = "responses"
                RESOLVED_STOP_OK = True
                AI_FUNCTIONAL_ERROR_MSG = None
                logging.info("OpenAI Codex OAuth runtime ready via Codex CLI")
            elif codex_mode_enabled and _openai_codex_any_profile_present():
                ai_provider_ready = False
                RESOLVED_MODEL = (openai_model or "").strip() or "codex"
                RESOLVED_PARAM_STYLE = "responses"
                RESOLVED_STOP_OK = True
                AI_FUNCTIONAL_ERROR_MSG = codex_reason or "OpenAI Codex OAuth profile exists but runtime is not ready"
                log_reason = str(AI_FUNCTIONAL_ERROR_MSG or "").strip().lower()
                if "still initializing" in log_reason or "not ready" in log_reason:
                    logging.info(
                        "OpenAI Codex OAuth runtime deferred until first on-demand token check: %s",
                        AI_FUNCTIONAL_ERROR_MSG,
                    )
                else:
                    logging.warning("OpenAI Codex OAuth runtime unavailable: %s", AI_FUNCTIONAL_ERROR_MSG)
            elif openai_client:
                ai_provider_ready = True
                RESOLVED_MODEL = (openai_model or "").strip() or "gpt-4o-mini"
                RESOLVED_PARAM_STYLE = "mct"
                RESOLVED_STOP_OK = True
                AI_FUNCTIONAL_ERROR_MSG = None
                logging.info("OpenAI API client available for non-Codex fallback contexts")
            elif codex_mode_enabled:
                logging.info("No OpenAI Codex OAuth profile found; OAuth mode configured but not connected.")
            else:
                logging.info("OpenAI Codex OAuth mode disabled; runtime unavailable.")
        elif openai_client:
            # One model only: probe the configured model to learn token param style and
            # ensure we get parseable output for PMDA. We do not auto-fallback to other models.
            cand = (openai_model or "").strip() or "gpt-4o-mini"
            style = _probe_model(cand)
            if style and _probe_ai_choose_best_response(cand):
                RESOLVED_MODEL = cand
                RESOLVED_PARAM_STYLE = style
                AI_FUNCTIONAL_ERROR_MSG = None
                logging.info(
                    "Using OpenAI model '%s' (%s, stop_ok=%s)",
                    RESOLVED_MODEL,
                    RESOLVED_PARAM_STYLE,
                    RESOLVED_STOP_OK,
                )
            else:
                ai_provider_ready = False
                if getattr(_runtime_module(), "OPENAI_LAST_PROBE_401", False):
                    AI_FUNCTIONAL_ERROR_MSG = (
                        "OpenAI API key is invalid or expired (401 Unauthorized). "
                        "Check your key in Settings → AI and try again."
                    )
                else:
                    probe_detail = (getattr(_runtime_module(), "OPENAI_MODEL_PROBE_LAST_ERROR", "") or "").strip()
                    detail_suffix = f" Reason: {probe_detail}" if probe_detail else ""
                    AI_FUNCTIONAL_ERROR_MSG = (
                        f"AI disabled: OpenAI model '{cand}' failed PMDA preflight (unsupported params or empty/unparseable output)."
                        f"{detail_suffix}"
                    )
                RESOLVED_MODEL = cand
                RESOLVED_PARAM_STYLE = "mct"
                RESOLVED_STOP_OK = True
                logging.warning("OpenAI model probe failed; AI disabled. %s", AI_FUNCTIONAL_ERROR_MSG)
        elif codex_mode_enabled:
            logging.info("No OPENAI_API_KEY; API-key mode unavailable (OAuth mode may still be usable).")
        else:
            logging.info("No OPENAI_API_KEY; AI-driven selection disabled.")
    elif provider == "anthropic":
        if anthropic_client:
            ai_provider_ready = True
        else:
            logging.info("No ANTHROPIC_API_KEY; AI-driven selection disabled.")
    elif provider == "google":
        if google_client_configured and google_client:
            ai_provider_ready = True
        else:
            logging.info("No GOOGLE_API_KEY; AI-driven selection disabled.")
    elif provider == "ollama":
        if ollama_url:
            ai_provider_ready = True
        else:
            logging.info("No OLLAMA_URL; AI-driven selection disabled.")
    else:
        logging.warning("Unknown AI_PROVIDER: %s", provider)

    if not ai_provider_ready and _local_first_scan_ai_enabled():
        ai_provider_ready = True
        AI_FUNCTIONAL_ERROR_MSG = None
        logging.info("Local-first scan AI is ready via Ollama")

    if not ai_provider_ready:
        ai_provider_ready = bool(openai_client or anthropic_client or (google_client_configured and google_client) or ollama_url)


def _reload_ai_config_and_reinit():
    """Reload AI config from settings.db and run _reinit_ai_from_globals().

    Used at scan start and provider preflight. This must read from SETTINGS_DB_FILE
    (single source of truth for configuration), not STATE_DB_FILE (scan data DB).
    """
    mod = _runtime_module()
    ai_config_keys = (
        "AI_PROVIDER",
        "AI_USAGE_LEVEL",
        "OPENAI_API_KEY",
        "OPENAI_MODEL",
        "OPENAI_ENABLE_API_KEY_MODE",
        "OPENAI_ENABLE_CODEX_OAUTH_MODE",
        "ANTHROPIC_API_KEY",
        "GOOGLE_API_KEY",
        "OLLAMA_URL",
        "OLLAMA_MODEL",
        "OLLAMA_COMPLEX_MODEL",
        "WEB_SEARCH_PROVIDER",
        "USE_AI_WEB_SEARCH_FALLBACK",
    )
    try:
        db_path = SETTINGS_DB_FILE
        if db_path.exists():
            con = sqlite3.connect(str(db_path), timeout=5)
            cur = con.cursor()
            placeholders = ",".join("?" for _ in ai_config_keys)
            cur.execute(
                f"SELECT key, value FROM settings WHERE key IN ({placeholders})",
                ai_config_keys,
            )
            for key, value in cur.fetchall():
                setattr(mod, key, (value or ""))
            con.close()
        # AI_USAGE_LEVEL is the single UX control. Re-apply its runtime overrides
        # on every scan start so stale legacy booleans from settings.db cannot
        # silently disable MB candidate arbitration or local web-assisted flows.
        try:
            _apply_ai_usage_level(getattr(mod, "AI_USAGE_LEVEL", "auto"))
        except Exception:
            logging.debug("Failed to re-apply AI usage level during AI reload", exc_info=True)
        _reinit_ai_from_globals()
    except Exception as e:
        logging.warning("_reload_ai_config_and_reinit failed: %s", e)


def _wait_for_codex_runtime_ready_for_scan(user_id: int | None = None, *, timeout_sec: float = 45.0) -> tuple[bool, str]:
    """
    When PMDA is configured to use Codex OAuth for scan-time AI, do not start the
    heavy pipeline until the OAuth runtime is actually usable.
    """
    uid = int(user_id or 0)
    if not _openai_codex_oauth_mode_enabled():
        return False, _provider_mode_disabled_reason("openai-codex") or "OpenAI Codex OAuth mode is disabled"
    if not (_openai_codex_profile_present(uid) or _openai_codex_any_profile_present()):
        return False, "No active OpenAI Codex OAuth profile"
    deadline = time.time() + max(5.0, float(timeout_sec or 0.0))
    last_reason = ""
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        ok, reason = _openai_codex_token_health(uid, force_refresh=(attempt == 1))
        if ok:
            return True, ""
        last_reason = str(reason or "").strip()
        time.sleep(1.0)
    return False, (last_reason or f"OpenAI Codex runtime not ready after {int(max(5.0, float(timeout_sec or 0.0)))}s")


# --- Resolve a working OpenAI model (with price-aware fallbacks) -------------
RESOLVED_MODEL = None
# Param style for the resolved model: "mct" -> max_completion_tokens, "mt" -> max_tokens
RESOLVED_PARAM_STYLE = "mct"
# Some models (e.g. reasoning/o-series) do not accept "stop"; when False, call_ai_provider omits stop.
RESOLVED_STOP_OK = True
# When no OpenAI candidate passes probe, set by _reinit_ai_from_globals for 503 / preflight message.
AI_FUNCTIONAL_ERROR_MSG = None
# Set by _probe_model when last failure was 401 so we can show a clear message.
OPENAI_LAST_PROBE_401 = False
# Best-effort error detail captured during OpenAI model probe so the UI can show an actionable reason.
OPENAI_MODEL_PROBE_LAST_ERROR = ""

def _probe_model(model_name: str) -> str | None:
    """Return param style ("mct" or "mt") if a 1-line ping works, else None.
    Tries max_completion_tokens first, then max_tokens; each with stop.
    If both fail with unsupported_parameter, retries without stop (some models do not accept stop).
    Sets global RESOLVED_STOP_OK to False when the model only works without stop.
    """
    global RESOLVED_STOP_OK, OPENAI_LAST_PROBE_401, AI_FUNCTIONAL_ERROR_MSG
    global OPENAI_MODEL_PROBE_LAST_ERROR
    OPENAI_LAST_PROBE_401 = False
    OPENAI_MODEL_PROBE_LAST_ERROR = ""
    if not (OPENAI_API_KEY and openai_client):
        return None

    def _probe_call(probe_kind: str, **kwargs: Any) -> Any:
        started_at = time.time()
        response_obj: Any = None
        status = "failed"
        error_msg = ""
        try:
            response_obj = openai_client.chat.completions.create(**kwargs)
            status = "completed"
            return response_obj
        except Exception as e:
            error_msg = str(e)
            raise
        finally:
            recorder = globals().get("record_ai_usage")
            if callable(recorder):
                recorder(
                    provider="openai",
                    model=str(model_name or ""),
                    endpoint_kind="text",
                    analysis_type="other",
                    started_at=started_at,
                    status=status,
                    response_obj=response_obj,
                    image_inputs=0,
                    error=error_msg,
                    metadata={
                        "probe_kind": str(probe_kind or "").strip() or "model_probe",
                        "purpose": "model_probe",
                        "max_tokens": int(
                            kwargs.get("max_completion_tokens")
                            or kwargs.get("max_tokens")
                            or 0
                        ),
                    },
                )
    # GPT-5 family models are known to reject some Chat Completions params (notably "stop"),
    # and can spend short completion budgets entirely on reasoning, producing empty text.
    # Probe them with a larger completion budget and without stop, and require a visible "PONG".
    try:
        mlow = (model_name or "").strip().lower()
    except Exception:
        mlow = ""
    if mlow.startswith("gpt-5"):
        try:
            resp = _probe_call(
                "gpt5_minimal_no_stop",
                model=model_name,
                messages=[
                    # For o1+ models (including GPT-5), prefer "developer" over "system".
                    {"role": "developer", "content": "Reply with exactly: PONG"},
                    {"role": "user", "content": "ping"},
                ],
                # Reduce reasoning to avoid spending the entire budget on hidden tokens.
                # This both lowers cost and makes it far more likely to get visible output.
                reasoning_effort="minimal",
                max_completion_tokens=96,
            )
            txt = (resp.choices[0].message.content or "").strip() if resp and resp.choices else ""
            if txt.upper().startswith("PONG"):
                RESOLVED_STOP_OK = False
                return "mct"
            OPENAI_MODEL_PROBE_LAST_ERROR = "Probe returned empty/unexpected output for a strict 'PONG' ping"
            return None
        except Exception as e:
            msg = str(e)
            OPENAI_MODEL_PROBE_LAST_ERROR = msg[:240]
            if "401" in msg or "unauthorized" in msg.lower():
                OPENAI_LAST_PROBE_401 = True
            logging.debug("Model probe (gpt-5 no-stop) failed for %s: %s", model_name, msg)
            return None
    # Try with max_completion_tokens + stop
    try:
        _probe_call(
            "mct_with_stop",
            model=model_name,
            messages=[{"role": "user", "content": "ping"}],
            max_completion_tokens=8,
            stop=["\n"],
        )
        RESOLVED_STOP_OK = True
        return "mct"
    except Exception as e:
        msg = str(e)
        if "401" in msg or "unauthorized" in msg.lower():
            OPENAI_LAST_PROBE_401 = True
        OPENAI_MODEL_PROBE_LAST_ERROR = msg[:240]
        logging.debug("Model probe (mct+stop) failed for %s: %s", model_name, msg)
    # Try legacy max_tokens + stop
    try:
        _probe_call(
            "mt_with_stop",
            model=model_name,
            messages=[{"role": "user", "content": "ping"}],
            max_tokens=8,
            stop=["\n"],
        )
        RESOLVED_STOP_OK = True
        return "mt"
    except Exception as e2:
        msg2 = str(e2)
        if "401" in msg2 or "unauthorized" in msg2.lower():
            OPENAI_LAST_PROBE_401 = True
        OPENAI_MODEL_PROBE_LAST_ERROR = msg2[:240]
        logging.debug("Model probe (mt+stop) failed for %s: %s", model_name, msg2)
    # If both failed with unsupported_parameter, try without stop (some reasoning models reject stop)
    low_msg = (msg or "").lower()
    low_msg2 = (msg2 or "").lower()
    unsupported = (
        ("unsupported_parameter" in low_msg) or ("unsupported_parameter" in low_msg2)
        or ("unsupported" in low_msg and "parameter" in low_msg)
        or ("unsupported" in low_msg2 and "parameter" in low_msg2)
        or ("stop" in low_msg and "unsupported" in low_msg)
        or ("stop" in low_msg2 and "unsupported" in low_msg2)
    )
    if not unsupported:
        return None
    try:
        _probe_call(
            "mct_no_stop",
            model=model_name,
            messages=[{"role": "user", "content": "ping"}],
            max_completion_tokens=8,
        )
        RESOLVED_STOP_OK = False
        logging.debug("Model probe: %s works with max_completion_tokens, without stop", model_name)
        return "mct"
    except Exception:
        pass
    try:
        _probe_call(
            "mt_no_stop",
            model=model_name,
            messages=[{"role": "user", "content": "ping"}],
            max_tokens=8,
        )
        RESOLVED_STOP_OK = False
        logging.debug("Model probe: %s works with max_tokens, without stop", model_name)
        return "mt"
    except Exception:
        return None


def _probe_ai_choose_best_response(model_name: str) -> bool:
    """Verify OpenAI can return the strict choose-best response PMDA expects."""
    global OPENAI_MODEL_PROBE_LAST_ERROR
    mod = _runtime_module()
    provider = str(getattr(mod, "AI_PROVIDER", AI_PROVIDER) or "").strip().lower()
    if not (OPENAI_API_KEY and openai_client and AI_PROVIDER and provider == "openai"):
        return True

    system_msg = (
        "You are an expert digital-music librarian.\n"
        "OUTPUT RULES (must follow exactly):\n"
        "- Return ONE single line only.\n"
        "- The line must contain EXACTLY two '|' characters.\n"
        "- Format: <index>|<brief rationale>|<comma-separated extra tracks>\n"
        "- If there are no extra tracks, still include the final pipe but leave it empty.\n"
        "- Do not add any other text, do not explain, do not add extra lines.\n"
        "Example of valid outputs:\n"
        "0|Preferred lossless|"
    )
    user_msg = (
        "Candidate editions:\n"
        "0: fmt_score=1, bd=24, tracks=10, size_mb=200\n"
        "1: fmt_score=0, bd=16, tracks=10, size_mb=100\n"
    )
    try:
        txt = call_ai_provider(
            AI_PROVIDER,
            model_name,
            system_msg,
            user_msg,
            max_tokens=256,
            analysis_type="other",
        )
        if not txt:
            logging.debug("Functional probe: model %s returned empty response", model_name)
            OPENAI_MODEL_PROBE_LAST_ERROR = "Functional probe returned empty output for choose_best"
            return False
        lines = [line.strip() for line in txt.replace("```", "").splitlines() if line.strip()]
        txt = lines[0] if lines else txt
        txt = re.sub(r"^(answer|réponse)\s*:\s*", "", txt, flags=re.IGNORECASE).strip()
        match = re.match(r"^(\d+)\s*\|\s*(.*?)\s*\|\s*(.*)$", txt)
        if match:
            idx = int(match.group(1))
            if 0 <= idx <= 1:
                logging.debug("Functional probe: model %s returned parseable response (index=%s)", model_name, idx)
                return True
        logging.debug("Functional probe: model %s response not parseable: %r", model_name, txt[:80])
        OPENAI_MODEL_PROBE_LAST_ERROR = f"Functional probe output not parseable: {txt[:120]!r}"
        return False
    except Exception as exc:
        logging.debug("Functional probe failed for %s: %s", model_name, exc)
        OPENAI_MODEL_PROBE_LAST_ERROR = f"Functional probe exception: {str(exc)[:180]}"
        return False


def _get_ai_provider_preferences(user_id: int | None = None) -> dict[str, str]:
    uid = int(user_id or 0)
    out = dict(_PROVIDER_PREF_DEFAULTS)
    try:
        con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=5)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute(
            """
            SELECT interactive_provider_id, batch_provider_id, web_search_provider_id
            FROM ai_provider_preferences
            WHERE user_id = ?
            LIMIT 1
            """,
            (uid,),
        )
        row = cur.fetchone()
        if not row and uid != 0:
            cur.execute(
                """
                SELECT interactive_provider_id, batch_provider_id, web_search_provider_id
                FROM ai_provider_preferences
                WHERE user_id = 0
                LIMIT 1
                """
            )
            row = cur.fetchone()
        con.close()
        if row:
            out["interactive_provider_id"] = _normalize_provider_id(row["interactive_provider_id"], fallback=out["interactive_provider_id"])
            out["batch_provider_id"] = _normalize_provider_id(row["batch_provider_id"], fallback=out["batch_provider_id"])
            out["web_search_provider_id"] = _normalize_provider_id(row["web_search_provider_id"], fallback=out["web_search_provider_id"])
    except Exception:
        pass
    return out


def _save_ai_provider_preferences(
    *,
    user_id: int | None,
    interactive_provider_id: str,
    batch_provider_id: str,
    web_search_provider_id: str,
) -> dict[str, str]:
    uid = int(user_id or 0)
    now = int(time.time())
    prefs = {
        "interactive_provider_id": _normalize_provider_id(interactive_provider_id, fallback="openai-codex"),
        "batch_provider_id": _normalize_provider_id(batch_provider_id, fallback="openai-codex"),
        "web_search_provider_id": _normalize_provider_id(web_search_provider_id, fallback="openai-codex"),
    }
    init_settings_db()
    con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=10)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO ai_provider_preferences
        (user_id, interactive_provider_id, batch_provider_id, web_search_provider_id, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            interactive_provider_id = excluded.interactive_provider_id,
            batch_provider_id = excluded.batch_provider_id,
            web_search_provider_id = excluded.web_search_provider_id,
            updated_at = excluded.updated_at
        """,
        (
            uid,
            prefs["interactive_provider_id"],
            prefs["batch_provider_id"],
            prefs["web_search_provider_id"],
            now,
        ),
    )
    con.commit()
    con.close()
    return prefs


def _openai_api_key_mode_enabled() -> bool:
    return bool(getattr(_runtime_module(), "OPENAI_ENABLE_API_KEY_MODE", True))


def _openai_codex_oauth_mode_enabled() -> bool:
    return bool(getattr(_runtime_module(), "OPENAI_ENABLE_CODEX_OAUTH_MODE", True))


def _provider_mode_enabled(provider_id: str) -> bool:
    pid = _normalize_provider_id(provider_id, fallback="")
    if pid == "openai-api":
        return _openai_api_key_mode_enabled()
    if pid == "openai-codex":
        return _openai_codex_oauth_mode_enabled()
    return True


def _provider_mode_disabled_reason(provider_id: str) -> str:
    pid = _normalize_provider_id(provider_id, fallback="")
    if pid == "openai-api" and not _openai_api_key_mode_enabled():
        return "OpenAI API-key mode is disabled in Settings"
    if pid == "openai-codex" and not _openai_codex_oauth_mode_enabled():
        return "OpenAI Codex OAuth mode is disabled in Settings"
    return ""


def _openai_api_runtime_available() -> bool:
    if not _openai_api_key_mode_enabled():
        return False
    if openai_client:
        return True
    return bool(str(globals().get("OPENAI_API_KEY", "") or "").strip())


def _openai_codex_runtime_available(user_id: int | None = None, *, require_token: bool = True) -> bool:
    if not _openai_codex_oauth_mode_enabled():
        return False
    return bool(_openai_codex_connected(user_id, require_token=require_token))


def _resolve_provider_for_runtime(requested_provider: str, analysis_type: str, *, user_id: int | None = None) -> str:
    req = str(requested_provider or "").strip().lower()
    if req and req not in _OPENAI_PROVIDER_IDS:
        return _normalize_provider_id(req, fallback=req)
    uid = _current_user_id_or_zero() if user_id is None else max(0, int(user_id or 0))
    context = _ai_context_from_analysis_type(analysis_type)
    local_override = _resolve_local_first_provider_for_runtime(context, req, user_id=uid)
    if local_override:
        return local_override
    prefs = _get_ai_provider_preferences(uid)
    api_ready = _openai_api_runtime_available()
    codex_ready = _openai_codex_runtime_available(uid, require_token=True)
    codex_profile_present = _openai_codex_connected(uid, require_token=False)
    preferred = {
        "interactive": prefs.get("interactive_provider_id", "openai-codex"),
        "batch": prefs.get("batch_provider_id", "openai-codex"),
        "web_search": prefs.get("web_search_provider_id", "openai-codex"),
    }.get(context, prefs.get("batch_provider_id", "openai-codex"))
    preferred_norm = _normalize_provider_id(str(preferred or ""), fallback="")
    # Prefer the explicit provider when it is genuinely usable. For Codex OAuth,
    # a stored profile alone is not enough: if runtime token derivation fails and
    # API-key mode is available, fall back so PMDA keeps functioning.
    if preferred_norm in _OPENAI_PROVIDER_IDS and _provider_mode_enabled(preferred_norm):
        if preferred_norm == "openai-codex":
            if codex_ready:
                return "openai-codex"
            if api_ready:
                return "openai-api"
            if codex_profile_present:
                return "openai-codex"
        elif api_ready:
            return preferred_norm
        elif codex_ready:
            return "openai-codex"

    selected = select_provider_id(
        context=context,
        preferred=str(preferred or ""),
        # Selection should stay lightweight and resilient; runtime token validation
        # happens when a request is actually sent to the provider.
        codex_connected=bool(codex_ready or codex_profile_present),
        openai_api_enabled=bool(api_ready),
        openai_codex_enabled=_openai_codex_oauth_mode_enabled(),
    )
    normalized = _normalize_provider_id(selected, fallback="openai-api")
    if normalized == "openai-api" and not api_ready:
        if codex_ready or codex_profile_present:
            return "openai-codex"
    if normalized == "openai-codex" and not _openai_codex_oauth_mode_enabled():
        if api_ready:
            return "openai-api"
    return normalized


def _resolve_openai_client_for_runtime(provider_for_usage: str, user_id: int | None) -> tuple[Any | None, str, str]:
    pid = _normalize_provider_id(provider_for_usage, fallback="openai-api")
    oauth_reason = ""
    if pid == "openai-codex":
        if not _openai_codex_oauth_mode_enabled():
            return None, "oauth", _provider_mode_disabled_reason("openai-codex") or "OpenAI Codex OAuth mode is disabled in Settings"
        derived_api_key = _get_openai_codex_exchanged_api_key()
        if derived_api_key:
            base_url = str(_get_config_from_db("OPENAI_CODEX_BASE_URL", "") or "").strip()
            kwargs: dict[str, Any] = {
                "api_key": derived_api_key,
                "timeout": _openai_request_timeout_seconds(),
            }
            if base_url:
                kwargs["base_url"] = base_url
            return OpenAI(**kwargs), "oauth", ""
        try:
            _run_callable_bounded(
                _openai_auth_service().get_valid_access_token_for_runtime,
                user_id,
                provider_id="openai-codex",
                ensure_runtime_key=True,
                timeout_sec=max(1.0, float(PMDA_OPENAI_CODEX_STATUS_TIMEOUT_SEC)),
                log_prefix="[OpenAI Codex runtime]",
            )
            derived_api_key = _get_openai_codex_exchanged_api_key()
            if derived_api_key:
                base_url = str(_get_config_from_db("OPENAI_CODEX_BASE_URL", "") or "").strip()
                kwargs = {
                    "api_key": derived_api_key,
                    "timeout": _openai_request_timeout_seconds(),
                }
                if base_url:
                    kwargs["base_url"] = base_url
                return OpenAI(**kwargs), "oauth", ""
            oauth_reason = "OpenAI Codex OAuth is connected but no runtime key was derived; reconnect Codex OAuth"
        except Exception as exc:
            oauth_reason = str(exc or "").strip() or "OpenAI Codex OAuth token is unavailable"
            if _openai_api_key_mode_enabled():
                logging.warning("OpenAI Codex OAuth unavailable; falling back to API key: %s", oauth_reason)
            else:
                logging.warning("OpenAI Codex OAuth unavailable (API-key mode disabled): %s", oauth_reason)
        if not _openai_api_key_mode_enabled():
            return None, "oauth", oauth_reason or "OpenAI Codex OAuth is connected but no usable runtime key is available; reconnect Codex OAuth"
    if not _openai_api_key_mode_enabled():
        return None, "api_key", _provider_mode_disabled_reason("openai-api") or "OpenAI API-key mode is disabled in Settings"
    if openai_client:
        return openai_client, "api_key", ""
    if pid == "openai-codex" and oauth_reason:
        return None, "none", oauth_reason
    return None, "none", "OpenAI API key is missing or invalid"


def _resolve_ai_runtime_availability(
    *,
    analysis_type: str,
    requested_provider: str = "openai",
    user_id: int | None = None,
) -> tuple[bool, str, str, str]:
    uid = _current_user_id_or_zero() if user_id is None else max(0, int(user_id or 0))
    provider_effective = _resolve_provider_for_runtime(
        requested_provider=str(requested_provider or ""),
        analysis_type=str(analysis_type or ""),
        user_id=uid,
    )
    provider_lower = str(provider_effective or "").strip().lower()
    auth_mode = _provider_auth_mode(provider_effective)
    if not _provider_mode_enabled(provider_effective):
        return (False, provider_effective, auth_mode, _provider_mode_disabled_reason(provider_effective) or "Provider mode disabled")
    if provider_lower == "openai-codex":
        ok, reason = _openai_codex_token_health(uid, force_refresh=False)
        return (
            bool(ok),
            provider_effective,
            auth_mode,
            str(reason or "") if not ok else "",
        )
    if provider_lower in {"openai", "openai-api", "openai-codex"}:
        client_to_use, auth_mode_for_usage, openai_reason = _resolve_openai_client_for_runtime(provider_effective, uid)
        if client_to_use:
            return (True, provider_effective, auth_mode_for_usage, "")
        return (
            False,
            provider_effective,
            auth_mode_for_usage,
            openai_reason or "OpenAI API key is missing or invalid",
        )
    if provider_lower == "anthropic":
        return (bool(anthropic_client), provider_effective, auth_mode, "Anthropic API key is missing or invalid")
    if provider_lower == "google":
        return (bool(google_client_configured and google_client), provider_effective, auth_mode, "Google API key is missing or invalid")
    if provider_lower == "ollama":
        return (bool(ollama_url), provider_effective, auth_mode, "Ollama URL is not configured")
    return (False, provider_effective, auth_mode, "Unsupported AI provider")


def _openai_chat_text(resp: Any) -> str:
    try:
        content = resp.choices[0].message.content
    except Exception:
        content = None
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                txt = item.get("text")
                if isinstance(txt, str):
                    parts.append(txt)
            else:
                txt = getattr(item, "text", None)
                if isinstance(txt, str):
                    parts.append(txt)
        return " ".join([p.strip() for p in parts if p and p.strip()]).strip()
    return (content or "").strip()


ANTHROPIC_COMPATIBLE_MODELS = [
    "claude-sonnet-4-5",
    "claude-haiku-4-5",
    "claude-opus-4-5",
    "claude-3-5-sonnet-20241022",
    "claude-3-5-sonnet-20240620",
    "claude-3-opus-20240229",
    "claude-3-sonnet-20240229",
    "claude-3-haiku-20240307",
]


GOOGLE_COMPATIBLE_MODELS = [
    "gemini-3-pro-preview",
    "gemini-3-flash-preview",
    "gemini-2.5-flash",
    "gemini-2.5-pro",
    "gemini-2.5-flash-lite",
    "gemini-2.0-flash",
    "gemini-2.0-flash-lite",
    "gemini-1.5-pro",
    "gemini-1.5-flash",
    "gemini-1.5-flash-8b",
    "gemini-pro",
]


def api_openai_check():
    """Test an OpenAI API key without persisting it."""
    data = request.get_json(silent=True) or {}
    key = (data.get("OPENAI_API_KEY") or "").strip() or OPENAI_API_KEY
    if not key:
        return jsonify({"success": False, "message": "OPENAI_API_KEY is required"}), 400

    if not key.startswith("sk-"):
        return jsonify({"success": False, "message": "Invalid API key format. OpenAI keys start with 'sk-'"}), 400

    try:
        client = OpenAI(api_key=key, timeout=_openai_request_timeout_seconds())
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": "OK"}],
                max_completion_tokens=5,
            )
            if response.choices and len(response.choices) > 0:
                return jsonify({"success": True, "message": "OpenAI connection successful"})
        except Exception as first_error:
            error_msg = str(first_error)
            if "max_completion_tokens" in error_msg or "unsupported_parameter" in error_msg.lower():
                try:
                    response = client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[{"role": "user", "content": "OK"}],
                        max_tokens=5,
                    )
                    if response.choices and len(response.choices) > 0:
                        return jsonify({"success": True, "message": "OpenAI connection successful"})
                except Exception as second_error:
                    error_msg2 = str(second_error)
                    if "invalid_api_key" in error_msg2.lower() or "authentication" in error_msg2.lower() or "401" in error_msg2:
                        return jsonify({"success": False, "message": "Invalid API key. Please check your key and try again."}), 401
                    if "insufficient_quota" in error_msg2.lower() or "quota" in error_msg2.lower():
                        return jsonify({"success": False, "message": "API key has insufficient quota. Please check your OpenAI account billing."}), 402
                    logging.warning("OpenAI check failed with max_tokens: %s", error_msg2)
                    return jsonify({"success": False, "message": f"OpenAI API error: {error_msg2}"}), 500
            else:
                if "invalid_api_key" in error_msg.lower() or "authentication" in error_msg.lower() or "401" in error_msg:
                    return jsonify({"success": False, "message": "Invalid API key. Please check your key and try again."}), 401
                if "insufficient_quota" in error_msg.lower() or "quota" in error_msg.lower():
                    return jsonify({"success": False, "message": "API key has insufficient quota. Please check your OpenAI account billing."}), 402
                logging.warning("OpenAI check failed: %s", error_msg)
                return jsonify({"success": False, "message": f"OpenAI API error: {error_msg}"}), 500
    except Exception as exc:
        error_msg = str(exc)
        logging.error("OpenAI check exception: %s", error_msg)
        if "connection" in error_msg.lower() or "timeout" in error_msg.lower():
            return jsonify({"success": False, "message": "Connection to OpenAI API failed. Please check your internet connection."}), 503
        return jsonify({"success": False, "message": f"Error: {error_msg}"}), 500


def api_openai_models():
    """Return curated OpenAI model IDs compatible with PMDA."""
    from flask import g

    data = getattr(g, "ai_models_request_data", None) or request.get_json(silent=True) or {}
    provider = (data.get("AI_PROVIDER") or "").strip().lower() or AI_PROVIDER.lower()
    if provider == "openai-codex":
        return jsonify(["codex"])
    key = (data.get("OPENAI_API_KEY") or "").strip() or OPENAI_API_KEY

    if not key:
        return jsonify({"error": "OPENAI_API_KEY is required"}), 400
    if not key.startswith("sk-"):
        return jsonify({"error": "Invalid API key format. OpenAI keys start with 'sk-'"}), 400

    try:
        OpenAI(api_key=key, timeout=_openai_request_timeout_seconds())
        available_models = list(OPENAI_COMPATIBLE_MODELS)
        logging.info("Returning %d compatible OpenAI models for Settings", len(available_models))
        return jsonify(available_models)
    except Exception as exc:
        error_msg = str(exc)
        logging.error("OpenAI client init for models list: %s", error_msg)
        if "invalid_api_key" in error_msg.lower() or "authentication" in error_msg.lower() or "401" in error_msg or "unauthorized" in error_msg.lower():
            return jsonify({"error": "Invalid API key. Please check your key and try again."}), 401
        return jsonify({"error": f"Failed to initialize OpenAI client: {error_msg}"}), 500


def api_anthropic_models():
    """Return curated Anthropic model IDs compatible with PMDA."""
    from flask import g

    data = getattr(g, "ai_models_request_data", None) or request.get_json(silent=True) or {}
    key = (data.get("ANTHROPIC_API_KEY") or "").strip() or ANTHROPIC_API_KEY

    if not key:
        return jsonify({"error": "ANTHROPIC_API_KEY is required"}), 400
    if not anthropic:
        return jsonify({"error": "Anthropic SDK not installed. Please install anthropic package."}), 500

    try:
        client = anthropic.Anthropic(api_key=key)
        try:
            client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=1,
                messages=[{"role": "user", "content": "test"}],
            )
        except anthropic.APIError as exc:
            if exc.status_code == 401:
                return jsonify({"error": "Invalid API key. Please check your key and try again."}), 401
            if exc.status_code == 402:
                return jsonify({"error": "API key has insufficient quota. Please check your Anthropic account billing."}), 402

        available_models = list(ANTHROPIC_COMPATIBLE_MODELS)
        logging.info("Returning %d compatible Anthropic models for Settings", len(available_models))
        return jsonify(available_models)
    except Exception as exc:
        error_msg = str(exc)
        logging.error("Failed to fetch Anthropic models: %s", error_msg)
        if "invalid_api_key" in error_msg.lower() or "authentication" in error_msg.lower() or "401" in error_msg or "unauthorized" in error_msg.lower():
            return jsonify({"error": "Invalid API key. Please check your key and try again."}), 401
        if "connection" in error_msg.lower() or "timeout" in error_msg.lower() or "network" in error_msg.lower():
            return jsonify({"error": "Connection to Anthropic API failed. Please check your internet connection."}), 503
        return jsonify({"error": f"Failed to fetch models: {error_msg}"}), 500


def api_google_models():
    """Return curated Google Gemini model IDs compatible with PMDA."""
    from flask import g

    data = getattr(g, "ai_models_request_data", None) or request.get_json(silent=True) or {}
    key = (data.get("GOOGLE_API_KEY") or "").strip() or GOOGLE_API_KEY

    if not key:
        return jsonify({"error": "GOOGLE_API_KEY is required"}), 400
    if not genai:
        return jsonify({"error": "Google GenAI SDK not installed. Please install google-genai package."}), 500

    try:
        try:
            client = genai.Client(api_key=key)
            models_list = client.models.list()
        except Exception as exc:
            error_msg = str(exc)
            if "invalid_api_key" in error_msg.lower() or "authentication" in error_msg.lower() or "401" in error_msg or "unauthorized" in error_msg.lower():
                return jsonify({"error": "Invalid API key. Please check your key and try again."}), 401
            raise

        compatible_set = set(GOOGLE_COMPATIBLE_MODELS)
        available_models: list[str] = []
        for model in models_list:
            model_name = getattr(model, "name", None) or ""
            model_id = model_name.split("/")[-1] if "/" in model_name else model_name
            supported = getattr(model, "supported_actions", None)
            if supported is None:
                supported = getattr(model, "supported_generation_methods", [])
            if model_id in compatible_set and (not supported or "generateContent" in str(supported)):
                if model_id not in available_models:
                    available_models.append(model_id)

        if not available_models:
            available_models = list(GOOGLE_COMPATIBLE_MODELS)

        def model_sort_key(name: str) -> tuple:
            if "gemini-3" in name:
                tier = 0
            elif "gemini-2.5" in name:
                tier = 1
            elif "gemini-2.0" in name:
                tier = 2
            elif "gemini-1.5" in name:
                tier = 3
            elif "gemini-pro" in name:
                tier = 4
            else:
                tier = 5
            return (tier, name)

        available_models.sort(key=model_sort_key)
        logging.info("Returning %d compatible Google Gemini models for Settings", len(available_models))
        return jsonify(available_models)
    except Exception as exc:
        error_msg = str(exc)
        logging.error("Failed to fetch Google models: %s", error_msg)
        if "invalid_api_key" in error_msg.lower() or "authentication" in error_msg.lower() or "401" in error_msg or "unauthorized" in error_msg.lower():
            return jsonify({"error": "Invalid API key. Please check your key and try again."}), 401
        if "connection" in error_msg.lower() or "timeout" in error_msg.lower() or "network" in error_msg.lower():
            return jsonify({"error": "Connection to Google API failed. Please check your internet connection."}), 503
        return jsonify({"error": f"Failed to fetch models: {error_msg}"}), 500


def _local_network_ipv4_candidates() -> list[str]:
    found: list[str] = []
    seen: set[str] = set()

    def _add(ip_text: str) -> None:
        raw = str(ip_text or "").strip()
        if not raw:
            return
        try:
            ip_obj = ipaddress.ip_address(raw)
        except Exception:
            return
        if ip_obj.version != 4 or ip_obj.is_loopback or ip_obj.is_multicast:
            return
        if raw in seen:
            return
        seen.add(raw)
        found.append(raw)

    try:
        with open("/proc/net/arp", "r", encoding="utf-8", errors="ignore") as handle:
            for idx, line in enumerate(handle):
                if idx == 0:
                    continue
                parts = [part for part in line.strip().split() if part]
                if parts:
                    _add(parts[0])
    except Exception:
        pass

    try:
        udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp.connect(("8.8.8.8", 80))
        local_ip = str(udp.getsockname()[0] or "").strip()
        udp.close()
        _add(local_ip)
        if local_ip.count(".") == 3:
            prefix = ".".join(local_ip.split(".")[:3])
            for suffix in (1, 2, 10, 20, 100, 254):
                _add(f"{prefix}.{suffix}")
    except Exception:
        pass

    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET, socket.SOCK_STREAM):
            sockaddr = info[4] or ()
            if sockaddr:
                _add(str(sockaddr[0] or "").strip())
    except Exception:
        pass

    return found


def api_ai_models():
    """Route to the active AI provider model list."""
    data = request.get_json(silent=True) or {}
    provider = (data.get("AI_PROVIDER") or "").strip().lower() or AI_PROVIDER.lower()

    from flask import g

    g.ai_models_request_data = data

    if provider in {"openai", "openai-api", "openai-codex"}:
        return api_openai_models()
    if provider == "anthropic":
        return api_anthropic_models()
    if provider == "google":
        return api_google_models()
    if provider == "ollama":
        return api_ollama_models()
    return jsonify({"error": f"Unknown AI provider: {provider}"}), 400


def _ollama_prewarm_model(
    model_name: str,
    *,
    analysis_type: str = "",
    force: bool = False,
) -> bool:
    host = str(getattr(_runtime_module(), "OLLAMA_URL", "") or "").strip().rstrip("/")
    model = str(model_name or "").strip()
    if not host or not model:
        return False
    context = _ai_context_from_analysis_type(str(analysis_type or ""))
    key = (model.lower(), context)
    now = time.time()
    with _OLLAMA_PREWARM_STATE_LOCK:
        cached = dict(_OLLAMA_PREWARM_STATE.get(key) or {})
        if (
            not force
            and float(cached.get("expires_at") or 0.0) > now
            and str(cached.get("status") or "") == "ready"
        ):
            return True
    keep_alive = _ollama_keep_alive_for_analysis(analysis_type=analysis_type, model_name=model)
    timeout_sec = 90 if model.lower() == _ollama_complex_model_configured().strip().lower() else 45
    payload = {
        "model": model,
        "prompt": "",
        "stream": False,
        "keep_alive": keep_alive,
        "options": {
            "num_predict": 0,
            "temperature": 0,
        },
    }
    try:
        resp = requests.post(
            f"{host}/api/generate",
            json=payload,
            timeout=timeout_sec,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"prewarm_http_{resp.status_code}")
        with _OLLAMA_PREWARM_STATE_LOCK:
            _OLLAMA_PREWARM_STATE[key] = {
                "status": "ready",
                "updated_at": now,
                "expires_at": now + _OLLAMA_PREWARM_CACHE_TTL_SEC,
                "keep_alive": keep_alive,
            }
        return True
    except Exception as exc:
        logging.info(
            "[OLLAMA] prewarm failed model=%s analysis=%s context=%s error=%s",
            model,
            str(analysis_type or "").strip() or "unspecified",
            context,
            exc,
        )
        with _OLLAMA_PREWARM_STATE_LOCK:
            _OLLAMA_PREWARM_STATE[key] = {
                "status": "failed",
                "updated_at": now,
                "expires_at": now + 30.0,
                "keep_alive": keep_alive,
                "error": str(exc),
            }
        return False


def _ollama_route_for_analysis(
    *,
    requested_model: str,
    analysis_type: str,
    endpoint_kind: str,
    system_msg: str,
    user_msg: str,
) -> tuple[str, dict[str, Any]]:
    base_model = str(requested_model or "").strip() or _ollama_model_configured()
    hard_model = _ollama_complex_model_configured()
    analysis = str(analysis_type or "").strip().lower()
    route_meta: dict[str, Any] = {
        "route": "bulk",
        "reason": "default",
        "base_model": base_model,
        "hard_model": hard_model,
    }
    if not hard_model or hard_model == base_model:
        route_meta["reason"] = "single_model"
        return base_model, route_meta

    score = 0
    reasons: list[str] = []
    if endpoint_kind == "longform":
        score += 3
        reasons.append("longform")
    if analysis in _OLLAMA_COMPLEX_ANALYSIS_TYPES:
        score += 3
        reasons.append(f"analysis:{analysis}")
    elif analysis in _OLLAMA_AMBIGUOUS_ESCALATION_ANALYSIS_TYPES:
        score += 1
        reasons.append(f"analysis:{analysis}")
    combined = f"{str(system_msg or '')}\n{str(user_msg or '')}".strip()
    if len(combined) >= 2400:
        score += 1
        reasons.append("prompt_size")
    if combined.count("\n") >= 18:
        score += 1
        reasons.append("prompt_density")
    if any(token in combined.lower() for token in _OLLAMA_COMPLEXITY_HINTS):
        score += 1
        reasons.append("ambiguity_hint")

    threshold = 3
    if analysis in _OLLAMA_AMBIGUOUS_ESCALATION_ANALYSIS_TYPES:
        threshold = 2
    if score < threshold:
        route_meta["reason"] = ",".join(reasons) if reasons else "not_complex"
        return base_model, route_meta
    if not _ollama_model_available(hard_model):
        route_meta["reason"] = "hard_model_missing"
        return base_model, route_meta

    route_meta["route"] = "hard_case"
    route_meta["reason"] = ",".join(reasons) if reasons else "complex"
    return hard_model, route_meta


_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals()[name] for name in _EXTRACTED_NAMES}


def _reinit_ai_from_globals_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _reinit_ai_from_globals(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _reload_ai_config_and_reinit_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _reload_ai_config_and_reinit(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _wait_for_codex_runtime_ready_for_scan_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _wait_for_codex_runtime_ready_for_scan(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _probe_model_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _probe_model(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _probe_ai_choose_best_response_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _probe_ai_choose_best_response(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _openai_request_timeout_seconds_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _openai_request_timeout_seconds(*args, **kwargs)

def _normalize_provider_id_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _normalize_provider_id(*args, **kwargs)

def _provider_auth_mode_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _provider_auth_mode(*args, **kwargs)

def _openai_error_allows_codex_fallback_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _openai_error_allows_codex_fallback(*args, **kwargs)

def _ai_context_from_analysis_type_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_context_from_analysis_type(*args, **kwargs)

def _get_ai_provider_preferences_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _get_ai_provider_preferences(*args, **kwargs)

def _save_ai_provider_preferences_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _save_ai_provider_preferences(*args, **kwargs)

def _openai_api_key_mode_enabled_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _openai_api_key_mode_enabled(*args, **kwargs)

def _openai_codex_oauth_mode_enabled_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _openai_codex_oauth_mode_enabled(*args, **kwargs)

def _provider_mode_enabled_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _provider_mode_enabled(*args, **kwargs)

def _provider_mode_disabled_reason_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _provider_mode_disabled_reason(*args, **kwargs)

def _openai_api_runtime_available_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _openai_api_runtime_available(*args, **kwargs)

def _openai_codex_runtime_available_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _openai_codex_runtime_available(*args, **kwargs)

def _resolve_provider_for_runtime_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _resolve_provider_for_runtime(*args, **kwargs)

def _resolve_ai_runtime_availability_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _resolve_ai_runtime_availability(*args, **kwargs)

def _resolve_openai_client_for_runtime_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _resolve_openai_client_for_runtime(*args, **kwargs)

def _openai_chat_text_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _openai_chat_text(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _ollama_prewarm_model_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ollama_prewarm_model(*args, **kwargs)

def _ollama_route_for_analysis_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ollama_route_for_analysis(*args, **kwargs)

def api_openai_check_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_openai_check(*args, **kwargs)

def api_openai_models_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_openai_models(*args, **kwargs)

def api_anthropic_models_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_anthropic_models(*args, **kwargs)

def api_google_models_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_google_models(*args, **kwargs)

def _local_network_ipv4_candidates_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _local_network_ipv4_candidates(*args, **kwargs)

def api_ai_models_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_ai_models(*args, **kwargs)
