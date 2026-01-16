from abc import abstractmethod
from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class LLMMessage:
    """Vendor-agnostic message DTO."""

    role: str  # "system" | "user" | "assistant"
    content: str


@dataclass
class LLMRequest:
    """Vendor-agnostic LLM request with provider-specific config support.

    This request format bridges multiple LLM providers. Some providers may use
    system_instruction separately, others may use it as a message. Adapters
    handle provider-specific transformation.
    """

    messages: list[LLMMessage]
    model: str
    max_completion_tokens: int = 500
    # Optional generation parameters (provider may ignore if not supported)
    temperature: float | None = None
    top_p: float | None = None
    top_k: float | None = None  # Gemini uses float for top_k
    # System instruction (some providers require it separate from messages)
    system_instruction: str | None = None
    # Provider-specific config (for future extensibility)
    provider_config: dict[str, Any] = field(default_factory=dict)


@dataclass
class LLMResponse:
    """Vendor-agnostic LLM response."""

    content: str
    model: str
    usage: dict[str, int]  # {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150}
    provider: str  # "portkey", "openai", etc.


class LLMClient(Protocol):
    """Port interface for LLM providers."""

    @abstractmethod
    def chat_completion(self, request: LLMRequest) -> LLMResponse:
        """Execute a chat completion request.

        Args:
            request: Vendor-agnostic LLM request

        Returns:
            LLMResponse with generated text

        Raises:
            utils.llm.error.LLMClientError: If API call fails
        """
        ...
