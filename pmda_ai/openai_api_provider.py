from __future__ import annotations

from typing import Iterator, Any

from .types import AIRequest, AIResponse, ProviderHealth


class OpenAIApiProvider:
    provider_id = "openai-api"
    auth_mode = "api_key"

    def __init__(self, client: Any | None = None) -> None:
        self._client = client

    def set_client(self, client: Any | None) -> None:
        self._client = client

    def generate(self, req: AIRequest) -> AIResponse:
        if self._client is None:
            raise RuntimeError("OpenAI API client is not initialized")
        resp = self._client.chat.completions.create(
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
        if self._client is None:
            return ProviderHealth(ok=False, message="OpenAI API client not configured")
        return ProviderHealth(ok=True, message="OpenAI API provider ready")
