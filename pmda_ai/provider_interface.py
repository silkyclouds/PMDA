from __future__ import annotations

from typing import Protocol, Iterator

from .types import AIRequest, AIResponse, ProviderHealth


class AIProvider(Protocol):
    provider_id: str
    auth_mode: str

    def generate(self, req: AIRequest) -> AIResponse:
        ...

    def generate_stream(self, req: AIRequest) -> Iterator[str]:
        ...

    def health(self) -> ProviderHealth:
        ...
