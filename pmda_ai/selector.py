from __future__ import annotations


def select_provider_id(
    *,
    context: str,
    preferred: str,
    codex_connected: bool,
    openai_api_enabled: bool = True,
    openai_codex_enabled: bool = True,
) -> str:
    """Select an effective provider ID for a given runtime context."""
    ctx = str(context or "batch").strip().lower()
    pref = str(preferred or "").strip().lower()

    def _enabled(pid: str) -> bool:
        if pid == "openai-api":
            return bool(openai_api_enabled)
        if pid == "openai-codex":
            return bool(openai_codex_enabled and codex_connected)
        return True

    if pref:
        if not _enabled(pref):
            # configured but unavailable -> fallback below
            pass
        else:
            return pref

    if ctx == "interactive":
        if _enabled("openai-codex"):
            return "openai-codex"
        if _enabled("openai-api"):
            return "openai-api"
        return "openai-api"
    if ctx == "web_search":
        if _enabled("openai-codex"):
            return "openai-codex"
        if _enabled("openai-api"):
            return "openai-api"
        return "openai-api"
    if _enabled("openai-api"):
        return "openai-api"
    if _enabled("openai-codex"):
        return "openai-codex"
    return "openai-api"
