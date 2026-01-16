import os

from zai import ZaiClient
from zai.core import APIStatusError, APITimeoutError

from utils.llm.error import LLMClientError
from utils.llm.llm_client import LLMClient, LLMRequest, LLMResponse
from utils.logging.logging_manager import LogManager


class ZAILLMAdapter(LLMClient):
    """Adapter for z.ai API using official SDK.

    Uses z.ai's official Python SDK for chat completions.
    Supports caching, retry logic, and comprehensive error handling.

    SDK Docs: https://docs.z.ai/guides/develop/python/introduction
    API Docs: https://docs.z.ai/api-reference/introduction
    """

    def __init__(
        self,
        api_key: str | None = None,
        timeout: float = 60.0,  # Request timeout in seconds
        max_retries: int = 3,  # Number of retry attempts
    ):
        """Initialize z.ai adapter with official SDK.

        Args:
            api_key: z.ai API key (defaults to Z_AI_API_KEY or ZAI_API_KEY env)
            timeout: Request timeout in seconds
            max_retries: Number of retry attempts on failure
        """
        self.api_key = api_key or os.getenv("Z_AI_API_KEY") or os.getenv("ZAI_API_KEY")

        if not self.api_key:
            raise ValueError("Z_AI_API_KEY or ZAI_API_KEY environment variable is required")

        self.logger = LogManager.get_instance().get_logger("ZAILLMAdapter")

        # Initialize z.ai client with official SDK
        # Note: ZaiClient automatically uses https://api.z.ai/api/paas/v4/ as base_url
        self.client = ZaiClient(
            api_key=self.api_key,
            timeout=timeout,
            max_retries=max_retries,
        )

        self.logger.info(f"z.ai LLM adapter initialized (timeout={timeout}s, max_retries={max_retries})")

    def chat_completion(self, request: LLMRequest) -> LLMResponse:
        """Execute chat completion via z.ai API using official SDK.

        Args:
            request: Vendor-agnostic LLM request

        Returns:
            LLMResponse with generated text

        Raises:
            LLMClientError: If API call fails after retries
        """
        try:
            self.logger.info(f"Calling z.ai API with model: {request.model}")

            # Call z.ai SDK
            response = self.client.chat.completions.create(
                model=request.model,
                messages=[{"role": m.role, "content": m.content} for m in request.messages],
                max_tokens=request.max_completion_tokens,
                temperature=0.7,  # Balanced temperature for classification
                stream=False,  # Non-streaming mode
            )

            # Extract content from response
            content = response.choices[0].message.content

            if not content:
                raise LLMClientError("z.ai API returned empty content")

            # Extract usage statistics
            usage = {
                "prompt_tokens": response.usage.prompt_tokens if hasattr(response, "usage") else 0,
                "completion_tokens": response.usage.completion_tokens if hasattr(response, "usage") else 0,
                "total_tokens": response.usage.total_tokens if hasattr(response, "usage") else 0,
            }

            # Build response object
            llm_response = LLMResponse(
                content=content,
                model=response.model,
                usage=usage,
                provider="zai",
            )

            self.logger.info(f"z.ai API call successful (tokens: {usage['total_tokens']})")

            return llm_response

        except APIStatusError as e:
            # Handle z.ai API status errors (4xx, 5xx)
            error_msg = f"z.ai API status error: {e.status_code} - {e.message}"
            self.logger.error(error_msg, exc_info=True)

            # Log rate limiting specifically
            if e.status_code == 429:
                self.logger.error("Rate limit exceeded. SDK will retry automatically with backoff.")

            raise LLMClientError(error_msg) from e

        except APITimeoutError as e:
            # Handle timeout errors
            error_msg = f"z.ai API timeout: {e}"
            self.logger.error(error_msg, exc_info=True)
            raise LLMClientError(error_msg) from e

        except Exception as e:
            # Handle unexpected errors
            error_msg = f"Unexpected z.ai API error: {e}"
            self.logger.error(error_msg, exc_info=True)
            raise LLMClientError(error_msg) from e
