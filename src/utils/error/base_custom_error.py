class BaseCustomError(Exception):
    """Base class for custom exceptions with metadata support."""

    def __init__(self, message: str, **metadata):
        """Initializes the exception with a message and optional metadata.

        :param message: Error message.
        :param metadata: Additional context or metadata for debugging.
        """
        super().__init__(message)
        self.metadata = metadata

    def __str__(self):
        metadata_info = ", ".join(f"{k}={v}" for k, v in self.metadata.items())
        return f"{super().__str__()} | Metadata: {metadata_info}"
