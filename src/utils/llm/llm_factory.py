import os

from utils.llm.gemini_adapter import GeminiLLMAdapter
from utils.llm.llm_client import LLMClient
from utils.llm.portkey_adapter import PortkeyLLMAdapter
from utils.llm.zai_adapter import ZAILLMAdapter
from utils.logging.logging_manager import LogManager


class LLMFactory:
    """Factory to create LLM clients based on configuration.

    Implements Composition Root pattern - centralizes
    dependency injection and provider selection.
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
            ValueError: If provider is unknown or config is invalid
        """
        provider = provider or os.getenv("LLM_PROVIDER", "portkey")

        LLMFactory._logger.info(f"Creating LLM client for provider: {provider}")

        if provider == "portkey":
            return PortkeyLLMAdapter()
        elif provider == "zai":
            return ZAILLMAdapter()
        elif provider == "gemini":
            return GeminiLLMAdapter()
        elif provider == "openai":
            # Future: return OpenAIAdapter()
            raise NotImplementedError("OpenAI adapter not yet implemented")
        else:
            raise ValueError(f"Unknown LLM provider: {provider}")
