from utils.error.base_custom_error import BaseCustomError


class SlackError(BaseCustomError):
    """Base exception class for all Slack related errors."""

    pass


class SlackApiError(SlackError):
    """Base exception for Slack API errors."""

    pass


class SlackApiRequestError(SlackApiError):
    """Raised when an API request to Slack fails."""

    def __init__(self, message: str, endpoint: str, status_code: int | None = None, **metadata):
        super().__init__(message, endpoint=endpoint, status_code=status_code, **metadata)


class SlackRateLimitError(SlackApiError):
    """Raised when Slack API returns a rate limit error (429)."""

    def __init__(self, retry_after: int, **metadata):
        super().__init__(f"Rate limited, retry after {retry_after}s", retry_after=retry_after, **metadata)


class SlackAuthenticationError(SlackApiError):
    """Raised when authentication with Slack fails."""

    def __init__(self, message: str = "Slack authentication failed", **metadata):
        super().__init__(message, **metadata)


class SlackConfigurationError(SlackError):
    """Raised when Slack configuration is missing or invalid."""

    def __init__(self, message: str = "Slack configuration error", **metadata):
        super().__init__(message, **metadata)
