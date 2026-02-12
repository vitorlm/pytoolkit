import os

from utils.env_loader import ensure_slack_env_loaded
from utils.slack.error import SlackConfigurationError


class SlackConfig:
    """Slack configuration manager."""

    def __init__(
        self, bot_token: str | None = None, webhook_url: str | None = None, default_channel: str | None = None
    ):
        """Initialize Slack configuration.

        Args:
            bot_token: Optional Slack bot token. If not provided, reads from SLACK_BOT_TOKEN env var.
            webhook_url: Optional Slack webhook URL. If not provided, reads from SLACK_WEBHOOK_URL env var.
            default_channel: Optional default Slack channel ID. If not provided, reads from SLACK_CHANNEL_ID env var.
        """
        # Ensure environment variables are loaded if we need to fall back to them
        if not all([bot_token, webhook_url, default_channel]):
            ensure_slack_env_loaded()

        self._bot_token = bot_token or os.getenv("SLACK_BOT_TOKEN")
        self._webhook_url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")
        self._default_channel = default_channel or os.getenv("SLACK_CHANNEL_ID")

        if not self._bot_token:
            raise SlackConfigurationError("SLACK_BOT_TOKEN environment variable is missing and no bot_token provided")

    @property
    def bot_token(self) -> str:
        """Get the Slack bot token."""
        return self._bot_token  # type: ignore

    @property
    def webhook_url(self) -> str | None:
        """Get the Slack webhook URL."""
        return self._webhook_url

    @property
    def default_channel(self) -> str | None:
        """Get the default Slack channel ID."""
        return self._default_channel
