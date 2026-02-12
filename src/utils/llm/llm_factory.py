import os

from utils.dependencies import require_optional, OptionalDependencyError
from utils.llm.llm_client import LLMClient
from utils.logging.logging_manager import LogManager


class LLMFactory:
    """Factory to create LLM clients based on configuration.

    Implements Composition Root pattern - centralizes
    dependency injection and provider selection.

    Uses lazy imports for optional LLM provider dependencies.
    """

    _logger = LogManager.get_instance().get_logger("LLMFactory")

    @staticmethod
    def create_client(provider: str | None = None) -> LLMClient:
        """Create LLM client based on provider name.

        Args:
            provider: Provider name ("portkey", "zai", "openai", etc.)
                     Defaults to LLM_PROVIDER env var

        Returns:
            Configured LLMClient instance

        Raises:
            OptionalDependencyError: If provider dependencies are not installed
            ValueError: If provider is unknown or config is invalid
        """
        provider = provider or os.getenv("LLM_PROVIDER", "portkey")

        LLMFactory._logger.info(f"Creating LLM client for provider: {provider}")

        if provider == "portkey":
            # Lazy import to avoid import-time failure if portkey-ai is not installed
            portkey_adapter = require_optional("utils.llm.portkey_adapter", "llm")
            return portkey_adapter.PortkeyLLMAdapter()
        elif provider == "zai":
            # Lazy import to avoid import-time failure if zai-sdk is not installed
            zai_adapter = require_optional("utils.llm.zai_adapter", "llm")
            return zai_adapter.ZAILLMAdapter()
        elif provider == "gemini":
            # Lazy import to avoid import-time failure if google-genai is not installed
            gemini_adapter = require_optional("utils.llm.gemini_adapter", "llm")
            return gemini_adapter.GeminiLLMAdapter()
        elif provider == "openai":
            # Future: return OpenAIAdapter()
            raise NotImplementedError("OpenAI adapter not yet implemented")
        else:
            raise ValueError(f"Unknown LLM provider: {provider}")
