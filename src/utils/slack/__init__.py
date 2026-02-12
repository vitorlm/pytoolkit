"""Slack utility package for PyToolkit.

Provides a decoupled, cached, and robust Slack client infrastructure matching
the project's architectural patterns.

Usage:
    >>> from utils.slack import SlackAssistant
    >>> assistant = SlackAssistant()
    >>> assistant.send_message(text="Hello from PyToolkit!")
"""

from utils.slack.error import (
    SlackApiError,
    SlackApiRequestError,
    SlackAuthenticationError,
    SlackConfigurationError,
    SlackError,
    SlackRateLimitError,
)
from utils.slack.slack_api_client import SlackApiClient
from utils.slack.slack_assistant import SlackAssistant
from utils.slack.slack_config import SlackConfig

__all__ = [
    "SlackApiClient",
    "SlackApiError",
    "SlackApiRequestError",
    "SlackAssistant",
    "SlackAuthenticationError",
    "SlackConfig",
    "SlackConfigurationError",
    "SlackError",
    "SlackRateLimitError",
]
