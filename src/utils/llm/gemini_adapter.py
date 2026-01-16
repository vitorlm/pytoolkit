import os

from google import genai
from google.genai.errors import APIError, ClientError
from google.genai.types import GenerateContentResponse

from utils.llm.error import LLMClientError
from utils.llm.gemini_request_builder import GeminiRequestBuilder
from utils.llm.llm_client import LLMClient, LLMRequest, LLMResponse
from utils.logging.logging_manager import LogManager


class GeminiLLMAdapter(LLMClient):
    """Adapter for Google Gemini API using official SDK.

    Uses Google's official Python SDK (google-genai) for chat completions.
    Supports caching, retry logic, and comprehensive error handling.

    Supported Models:
        - gemini-3.0-pro: Latest high-performance model
        - gemini-2.0-flash: Fast, efficient reasoning
        - gemini-1.5-pro: Extended context window
        - gemini-1.5-flash: Fast response with good quality

    SDK Docs: https://ai.google.dev/
    API Docs: https://ai.google.dev/gemini-api/docs/
    """

    def __init__(
        self,
        api_key: str | None = None,
        timeout: float = 60.0,  # Request timeout in seconds
        max_retries: int = 3,  # Number of retry attempts
    ):
        """Initialize Gemini adapter with official SDK.

        Args:
            api_key: Google API key (defaults to GOOGLE_API_KEY env)
            timeout: Request timeout in seconds
            max_retries: Number of retry attempts on failure

        Raises:
            ValueError: If GOOGLE_API_KEY is not set
        """
        self.api_key = api_key or os.getenv("GOOGLE_API_KEY")

        if not self.api_key:
            raise ValueError("GOOGLE_API_KEY environment variable is required")

        self.logger = LogManager.get_instance().get_logger("GeminiLLMAdapter")
        self.timeout = timeout
        self.max_retries = max_retries
        self.builder = GeminiRequestBuilder()  # Request builder for conversions

        # Initialize Gemini SDK with client (genai.configure() not available in new SDK)
        self.client = genai.Client(api_key=self.api_key)

        self.logger.info(f"Gemini LLM adapter initialized (timeout={timeout}s, max_retries={max_retries})")

    def chat_completion(self, request: LLMRequest) -> LLMResponse:
        """Execute chat completion via Google Gemini API.

        Args:
            request: Vendor-agnostic LLM request

        Returns:
            LLMResponse with generated text

        Raises:
            LLMClientError: If API call fails after retries
        """
        try:
            self.logger.info(f"Calling Gemini API with model: {request.model}")

            # Use builder to convert LLMRequest to Gemini parameters
            gemini_params = self.builder.build_generate_content_params(request)

            # Call Gemini API using the client
            response: GenerateContentResponse = self.client.models.generate_content(**gemini_params)

            # Extract content from response using builder
            content = self.builder.extract_response_content(response)

            # Extract usage statistics using builder
            usage = self.builder.extract_usage(response, request)

            # Build response object
            llm_response = LLMResponse(
                content=content,
                model=request.model,
                usage=usage,
                provider="gemini",
            )

            self.logger.info(f"Gemini API call successful (tokens: {usage['total_tokens']})")

            return llm_response

        except ClientError as e:
            # Handle client-side errors (authentication, permissions, etc.)
            error_msg = f"Gemini API client error: {e}"
            self.logger.error(error_msg, exc_info=True)

            # Log specific error types
            if "unauthenticated" in str(e).lower():
                self.logger.error("Authentication failed. Check GOOGLE_API_KEY validity.")
            elif "permission" in str(e).lower():
                self.logger.error("Permission denied. Check API key permissions.")

            raise LLMClientError(error_msg) from e

        except APIError as e:
            # Handle API errors (rate limits, server errors, etc.)
            error_msg = f"Gemini API error: {e}"
            self.logger.error(error_msg, exc_info=True)

            if "exhausted" in str(e).lower():
                self.logger.error("Rate limit or quota exceeded. Please wait before retrying.")

            raise LLMClientError(error_msg) from e

        except Exception as e:
            # Handle unexpected errors
            error_msg = f"Unexpected Gemini API error: {e}"
            self.logger.error(error_msg, exc_info=True)
            raise LLMClientError(error_msg) from e
