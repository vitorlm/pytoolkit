"""Adapter for Syngenta AI Foundry Gateway (Portkey).

This module requires portkey-ai to be installed.
Install with: pip install -e '.[llm]'
"""

import os

# Note: E402 suppressed via pyproject.toml per-file-ignores
# for optional dependency pattern (industry standard - pandas, sklearn, torch)
try:
    from portkey_ai import Portkey
    PORTKEY_AVAILABLE = True
except ImportError:
    PORTKEY_AVAILABLE = False
    Portkey = None  # type: ignore

from utils.llm.error import LLMClientError
from utils.llm.llm_client import LLMClient, LLMRequest, LLMResponse
from utils.logging.logging_manager import LogManager


class PortkeyLLMAdapter(LLMClient):
    """Adapter for Syngenta AI Foundry Gateway (Portkey).

    Uses Portkey SDK to route requests through Syngenta's AI Gateway.
    Model format: provider/model-name (e.g., openai/gpt-4o)

    Docs: https://syngenta-digital.github.io/aifoundry-portal/docs/gateway-api-calls
    """

    SYNGENTA_GATEWAY_URL = "https://portkey.syngenta.com/v1"

    def __init__(
        self,
        api_key: str | None = None,
    ):
        """Initialize Portkey adapter for Syngenta AI Foundry Gateway.

        Args:
            api_key: Portkey API key (defaults to PORTKEY_API_KEY env)

        Raises:
            ValueError: If PORTKEY_API_KEY is not set or portkey-ai not installed
        """
        if not PORTKEY_AVAILABLE:
            raise ImportError(
                "portkey-ai is required for PortkeyLLMAdapter.\n\n"
                "Install with: pip install -e '.[llm]'\n\n"
                "Documentation: https://github.com/user/PyToolkit#optional-dependencies"
            )

        self.api_key = api_key or os.getenv("PORTKEY_API_KEY")

        if not self.api_key:
            raise ValueError("PORTKEY_API_KEY is required")

        self.logger = LogManager.get_instance().get_logger("PortkeyLLMAdapter")

        # Initialize Portkey client with Syngenta Gateway URL
        self.client = Portkey(api_key=self.api_key, base_url=self.SYNGENTA_GATEWAY_URL)

    def chat_completion(self, request: LLMRequest) -> LLMResponse:
        """Execute chat completion via Syngenta AI Foundry Gateway.

        Model format: provider/model-name (e.g., openai/gpt-4o)
        The SDK will extract provider and set the x-portkey-provider header automatically.

        See available models: https://app.portkey.ai/model-catalog/models
        """
        try:
            # Parse model format: provider/model-name
            if "/" in request.model:
                provider, model_name = request.model.split("/", 1)
            else:
                # Fallback if no provider specified
                provider = "openai"
                model_name = request.model

            self.logger.info(f"Calling Syngenta AI Foundry Gateway with provider={provider}, model={model_name}")

            # Call Portkey SDK with provider header
            completion = self.client.with_options(provider=provider).chat.completions.create(
                model=model_name,
                messages=[{"role": m.role, "content": m.content} for m in request.messages],
                max_completion_tokens=request.max_completion_tokens,
            )

            # Parse response
            llm_response = LLMResponse(
                content=completion.choices[0].message.content or "",
                model=completion.model,
                usage=completion.usage.__dict__ if hasattr(completion, "usage") else {},
                provider="portkey",
            )

            return llm_response

        except Exception as e:
            self.logger.error(f"Syngenta AI Foundry Gateway call failed: {e}", exc_info=True)
            raise LLMClientError(f"AI Foundry Gateway error: {e}") from e
