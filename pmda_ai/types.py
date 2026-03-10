from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator


@dataclass(slots=True)
class AIRequest:
    model: str
    system_msg: str
    user_msg: str
    max_tokens: int = 256
    analysis_type: str = ""
    user_id: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AIResponse:
    text: str
    raw: Any = None
    provider_id: str = ""
    model: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ProviderHealth:
    ok: bool
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)


StreamChunk = Iterator[str]
