import os
from typing import Dict, List, Optional

from ollama import ChatResponse, Client, ResponseError

from log_config import log_manager


class OllamaAssistant:
    """
    A generic assistant using the Ollama language model for various text processing tasks.
    Includes core methods for text generation, summarization, translation, and custom queries.
    Designed to be extensible for specialized assistants.
    """

    _logger = log_manager.get_logger(
        module_name=os.path.splitext(os.path.basename(__file__))[0]
    )
    log_manager.add_custom_handler(
        logger_name="httpx", replace_existing=True, handler_id="ollama_httpx"
    )

    def __init__(
        self,
        host: str = "http://localhost:11434",
        model: str = "llama3.2",
        **kwargs,
    ):
        """
        Initializes the OllamaAssistant with specified parameters.

        Args:
            host (str): The address of the Ollama instance.
            model (str): The name of the Ollama model to use.
            **kwargs: Additional configuration parameters.

        Raises:
            ValueError: If the model configuration is invalid.
        """
        self._validate_model_configuration(host, model)
        self.client = Client(host=host)
        self.model = model
        self.config = kwargs

    def _validate_model_configuration(self, host: str, model: str) -> None:
        """
        Validates the model configuration before initializing the assistant.

        Args:
            host (str): The address of the Ollama instance.
            model (str): The name of the Ollama model to use.

        Raises:
            ValueError: If the host or model is invalid.
        """
        if not isinstance(host, str) or not host.startswith("http"):
            raise ValueError("Invalid host URL provided for Ollama instance.")
        if not isinstance(model, str) or not model:
            raise ValueError("Model name must be a non-empty string.")

    def generate_text(self, messages: List[Dict[str, str]]) -> str:
        """
        Generates text using the specified model and input messages.

        Args:
            messages (List[Dict[str, str]]): A list of messages in the format [{"role": "user", "content": "..."}].

        Returns:
            str: The generated text response.

        Raises:
            ResponseError: If the Ollama API returns an error.
            Exception: For other unexpected errors.
        """
        self._logger.info(f"Generating text with model {self.model}")
        try:
            response: ChatResponse = self.client.chat(
                model=self.model, messages=messages, options=self.config
            )
            self._logger.debug(f"Generated response: {response.message['content']}")
            return response.message["content"]
        except ResponseError as e:
            self._logger.error(
                f"Ollama API error: {e.error} (Status Code: {e.status_code})",
                exc_info=True,
            )
            raise
        except Exception as e:
            self._logger.error(f"Unexpected error in generate_text: {e}", exc_info=True)
            raise

    def summarize_text(self, text: str, context: Optional[str] = None) -> str:
        """
        Summarizes the given text with optional context.

        Args:
            text (str): The text to be summarized.
            context (Optional[str]): Additional context for the summarization.

        Returns:
            str: The summarized text.

        Raises:
            ValueError: If the input text is not a valid string.
            Exception: For unexpected errors during summarization.
        """
        if not isinstance(text, str) or not text.strip():
            raise ValueError("Input text must be a non-empty string.")

        self._logger.info("Summarizing text")
        try:
            messages = [
                {
                    "role": "system",
                    "content": "You are an assistant specializing in summarization.",
                },
                {"role": "user", "content": f"Summarize this text: {text}"},
            ]
            if context:
                messages.insert(1, {"role": "user", "content": f"Context: {context}"})
            return self.generate_text(messages)
        except ValueError as e:
            self._logger.error(f"Invalid input for summarization: {e}", exc_info=True)
            raise
        except Exception as e:
            self._logger.error(
                f"Unexpected error in summarize_text: {e}", exc_info=True
            )
            raise

    def translate_text(self, text: str, target_language: str) -> str:
        """
        Translates the given text to the specified target language.

        Args:
            text (str): The text to translate.
            target_language (str): The language to translate the text into.

        Returns:
            str: The translated text.

        Raises:
            ValueError: If the input text or target language is invalid.
            Exception: For unexpected errors during translation.
        """
        if not isinstance(text, str) or not text.strip():
            raise ValueError("Input text must be a non-empty string.")
        if not isinstance(target_language, str) or not target_language.strip():
            raise ValueError("Target language must be a valid string.")

        self._logger.info(f"Translating text to {target_language}")
        try:
            messages = [
                {
                    "role": "system",
                    "content": (
                        f"You are a translator who translates text directly into {target_language} "
                        "without providing explanations, introductions, or additional context."
                    ),
                },
                {
                    "role": "user",
                    "content": f"Translate this text to {target_language}: {text}",
                },
            ]

            return self.generate_text(messages)
        except Exception as e:
            self._logger.error(f"Error in translate_text: {e}", exc_info=True)
            raise

    def identify_languages(self, text: str) -> List[str]:
        """
        Identifies all languages present in the provided text.

        Args:
            text (str): The text whose languages are to be identified.

        Returns:
            List[str]: A list of detected languages.

        Raises:
            ValueError: If the input text is invalid.
            Exception: For unexpected errors during language identification.
        """
        if not isinstance(text, str) or not text.strip():
            raise ValueError("Input text must be a non-empty string.")

        self._logger.info("Identifying all languages in the given text")
        try:
            messages = [
                {
                    "role": "system",
                    "content": (
                        "You are a linguistic assistant specialized in language detection. "
                        "If the text contains multiple languages, identify each of them "
                        "and return their names in an array."
                    ),
                },
                {
                    "role": "user",
                    "content": f"Identify all languages in this text: {text}",
                },
            ]
            response = self.generate_text(messages)
            languages = response.split(
                ","
            )  # Split response assuming it's a comma-separated list
            return [
                lang.strip() for lang in languages
            ]  # Clean whitespace around language names
        except Exception as e:
            self._logger.error(f"Error in identify_languages: {e}", exc_info=True)
            raise
