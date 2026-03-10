from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from .provider_interface import AIProvider


@dataclass
class ProviderRegistry:
    _providers: dict[str, AIProvider] = field(default_factory=dict)

    def register(self, provider: AIProvider) -> None:
        pid = str(getattr(provider, "provider_id", "") or "").strip().lower()
        if not pid:
            raise ValueError("Provider must expose a non-empty provider_id")
        self._providers[pid] = provider

    def register_many(self, providers: Iterable[AIProvider]) -> None:
        for provider in providers:
            self.register(provider)

    def get(self, provider_id: str) -> AIProvider | None:
        pid = str(provider_id or "").strip().lower()
        return self._providers.get(pid)

    def ids(self) -> list[str]:
        return sorted(self._providers.keys())
