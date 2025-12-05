from log_config import log_manager

logger = log_manager.get_logger("ErrorManager")


def handle_generic_exception(exception: Exception, context_message: str, metadata: dict | None = None):
    """Handles generic exceptions with optional metadata.

    :param exception: The exception raised.
    :param context_message: Custom message providing context for the error.
    :param metadata: Additional metadata (optional) for debugging purposes.
    """
    metadata_info = f" | Metadata: {metadata}" if metadata else ""
    logger.error(
        f"An error occurred: {context_message}{metadata_info} - {exception}",
        exc_info=True,
    )
    raise Exception(f"{context_message}{metadata_info}")
