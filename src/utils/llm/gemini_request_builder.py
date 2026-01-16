"""Gemini-specific request builder for converting LLMRequest to Gemini SDK format.

The Gemini API has unique requirements:
1. System instructions must be passed in config, not as a message
2. Messages use "model" role instead of "assistant"
3. Messages must be wrapped in Content objects with Parts
4. Generation parameters go in GenerateContentConfig

This builder handles the transformation from vendor-agnostic LLMRequest to
Gemini-specific format, ensuring correct multi-turn conversation support.

Docs: https://googleapis.github.io/python-genai/
"""

from typing import Any

from google import genai

from utils.llm.llm_client import LLMRequest
from utils.logging.logging_manager import LogManager


class GeminiRequestBuilder:
    """Build Gemini SDK calls from vendor-agnostic LLMRequest."""

    def __init__(self) -> None:
        """Initialize builder."""
        self.logger = LogManager.get_instance().get_logger("GeminiRequestBuilder")

    def build_generate_content_params(self, request: LLMRequest) -> dict[str, Any]:
        """Convert LLMRequest to Gemini generate_content parameters.

        Args:
            request: Vendor-agnostic LLM request

        Returns:
            Dictionary with parameters for client.models.generate_content()

        Example:
            >>> request = LLMRequest(
            ...     messages=[LLMMessage(role="user", content="Hello")],
            ...     model="gemini-2.0-flash",
            ...     max_completion_tokens=100,
            ...     system_instruction="You are helpful",
            ... )
            >>> params = builder.build_generate_content_params(request)
            >>> response = client.models.generate_content(**params)
        """
        # Build contents array (messages)
        contents = self._build_contents(request.messages)

        # Build generation config
        config = self._build_config(request)

        return {
            "model": request.model,
            "contents": contents,
            "config": config,
        }

    def _build_contents(self, messages: list) -> list[genai.types.Content]:
        """Convert messages to Gemini Content objects.

        Transforms vendor-agnostic messages to Gemini format:
        - "assistant" role → "model" role
        - "user" role → "user" role (unchanged)
        - "system" role → filtered out (handled separately in config)
        - Each message wrapped in Content with Parts

        Args:
            messages: List of LLMMessage objects

        Returns:
            List of genai.types.Content objects ready for API call
        """
        contents: list[genai.types.Content] = []

        for msg in messages:
            # Skip system messages (they go in config.system_instruction)
            if msg.role == "system":
                self.logger.debug("Skipping system message; will use system_instruction in config")
                continue

            # Map vendor-agnostic role to Gemini role
            # Gemini uses "model" for LLM responses, not "assistant"
            role = "model" if msg.role == "assistant" else msg.role

            # Create Gemini Content with text part
            content = genai.types.Content(
                role=role,
                parts=[genai.types.Part(text=msg.content)],
            )
            contents.append(content)

        return contents

    def _build_config(self, request: LLMRequest) -> genai.types.GenerateContentConfig:
        """Build GenerateContentConfig from LLMRequest.

        Args:
            request: Vendor-agnostic request with optional config

        Returns:
            GenerateContentConfig for Gemini API

        Note:
            system_instruction can be str or ContentUnion (Content, Part, list[Part]).
            This method accepts str for simplicity; use provider_config for advanced
            Content structures.
        """
        # Prepare config kwargs (only include if specified)
        config_kwargs: dict[str, Any] = {
            "max_output_tokens": request.max_completion_tokens,
        }

        # Add optional generation parameters
        if request.temperature is not None:
            config_kwargs["temperature"] = request.temperature

        if request.top_p is not None:
            config_kwargs["top_p"] = request.top_p

        if request.top_k is not None:
            config_kwargs["top_k"] = request.top_k  # Must be float (Gemini types.py line 5037)

        # Add system instruction (unique to Gemini - passed in config, not messages)
        # Gemini accepts: str | Content | Part | list[Part]
        if request.system_instruction is not None:
            config_kwargs["system_instruction"] = request.system_instruction

        # Any provider-specific config overrides
        if request.provider_config:
            config_kwargs.update(request.provider_config)

        return genai.types.GenerateContentConfig(**config_kwargs)

    def extract_response_content(self, response: genai.types.GenerateContentResponse) -> str:
        """Extract text content from Gemini API response.

        Args:
            response: genai.types.GenerateContentResponse

        Returns:
            Text content from first candidate

        Raises:
            ValueError: If response has no text content
        """
        if not response or not hasattr(response, "text") or not response.text:
            raise ValueError("Gemini API returned empty content or no text in response")

        return response.text.strip()

    def extract_usage(self, response: genai.types.GenerateContentResponse, request: LLMRequest) -> dict[str, int]:
        """Extract token usage from Gemini response.

        Extracts token usage from GenerateContentResponseUsageMetadata (types.py line 6595):

        Primary fields (used):
        - prompt_token_count: tokens in the input prompt
        - candidates_token_count: tokens in the generated response
        - total_token_count: sum of both
        - cached_content_token_count: tokens from cached content

        Additional fields in metadata (available if needed):
        - cache_tokens_details: token breakdown by modality in cache
        - candidates_tokens_details: token breakdown by modality in response
        - prompt_tokens_details: token breakdown by modality in prompt
        - thoughts_token_count: tokens from model reasoning (if thinking model)
        - tool_use_prompt_token_count: tokens from tool call results
        - traffic_type: request traffic classification

        Falls back to estimation if usage_metadata is unavailable.

        Args:
            response: genai.types.GenerateContentResponse with usage_metadata
            request: Original LLM request (for fallback estimation)

        Returns:
            Dict with prompt_tokens, completion_tokens, total_tokens
        """
        # Try to extract from response metadata
        try:
            if hasattr(response, "usage_metadata") and response.usage_metadata is not None:
                usage_meta: genai.types.GenerateContentResponseUsageMetadata = response.usage_metadata

                # Extract token counts from metadata (all fields return 0 if None)
                prompt_tokens = int(usage_meta.prompt_token_count or 0)
                completion_tokens = int(usage_meta.candidates_token_count or 0)
                total_tokens = int(usage_meta.total_token_count or 0)

                # Validate total_tokens is correct (fallback to manual sum if needed)
                if total_tokens == 0 and (prompt_tokens > 0 or completion_tokens > 0):
                    total_tokens = prompt_tokens + completion_tokens

                self.logger.debug(
                    f"Token usage extracted: prompt={prompt_tokens}, "
                    f"completion={completion_tokens}, total={total_tokens}"
                )

                return {
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": total_tokens,
                }
        except Exception as e:
            self.logger.debug(f"Could not extract usage metadata from response: {e}")

        # Fallback: estimate tokens (roughly 4 chars ≈ 1 token)
        self.logger.debug("Using token estimation fallback (4 chars ≈ 1 token)")
        prompt_tokens = sum(len(msg.content) // 4 for msg in request.messages)
        response_text = getattr(response, "text", "")
        completion_tokens = len(response_text) // 4 if response_text else 0

        return {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
        }
