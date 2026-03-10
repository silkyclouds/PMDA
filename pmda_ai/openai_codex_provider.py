from __future__ import annotations

from typing import Any, Callable, Iterator

from openai import OpenAI

from .types import AIRequest, AIResponse, ProviderHealth


class OpenAICodexProvider:
    provider_id = "openai-codex"
    auth_mode = "oauth"

    def __init__(
        self,
        *,
        token_getter: Callable[[int | None], str],
        base_url: str = "",
    ) -> None:
        self._token_getter = token_getter
        self._base_url = str(base_url or "").strip()

    def _build_client(self, user_id: int | None) -> OpenAI:
        token = str(self._token_getter(user_id) or "").strip()
        if not token:
            raise RuntimeError("OpenAI Codex OAuth token unavailable")
        kwargs: dict[str, Any] = {"api_key": token}
        if self._base_url:
            kwargs["base_url"] = self._base_url
        return OpenAI(**kwargs)

    def generate(self, req: AIRequest) -> AIResponse:
        client = self._build_client(req.user_id)
        resp = client.chat.completions.create(
            model=req.model,
            messages=[
                {"role": "system", "content": req.system_msg},
                {"role": "user", "content": req.user_msg},
            ],
            max_tokens=req.max_tokens,
        )
        text = ""
        try:
            text = (resp.choices[0].message.content or "").strip()
        except Exception:
            text = ""
        return AIResponse(text=text, raw=resp, provider_id=self.provider_id, model=req.model)

    def generate_stream(self, req: AIRequest) -> Iterator[str]:
        out = self.generate(req)
        yield out.text

    def health(self) -> ProviderHealth:
        try:
            _ = self._token_getter(None)
            return ProviderHealth(ok=True, message="OpenAI Codex provider ready")
        except Exception as exc:
            return ProviderHealth(ok=False, message=str(exc) or "OpenAI Codex provider unavailable")
