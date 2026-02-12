from typing import Any

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from utils.logging.logging_manager import LogManager
from utils.slack.error import SlackApiRequestError, SlackAuthenticationError, SlackRateLimitError


class SlackApiClient:
    """Low-level Slack API client wrapping slack_sdk.WebClient for decoupling."""

    def __init__(self, bot_token: str):
        """Initialize the Slack API client.

        Args:
            bot_token: Slack bot token for authentication.
        """
        self.logger = LogManager.get_instance().get_logger("SlackApiClient")
        self.client = WebClient(token=bot_token)

    def _handle_error(self, e: SlackApiError, endpoint: str):
        """Handle Slack SDK specific errors and map them to custom exceptions.

        Args:
            e: The SlackApiError from the SDK.
            endpoint: The API endpoint being called.

        Raises:
            SlackRateLimitError: If rate limited (429).
            SlackAuthenticationError: If token is invalid.
            SlackApiRequestError: For other API errors.
        """
        error_type = e.response.get("error", "unknown")
        status_code = e.response.status_code

        if status_code == 429:
            retry_after = int(e.response.headers.get("Retry-After", 5))
            self.logger.warning(f"Slack rate limit hit on {endpoint}. Retry after {retry_after}s")
            raise SlackRateLimitError(retry_after=retry_after, endpoint=endpoint)

        if error_type in ["invalid_auth", "not_authed", "account_inactive", "token_revoked"]:
            self.logger.error(f"Slack authentication error on {endpoint}: {error_type}")
            raise SlackAuthenticationError(message=f"Slack auth error: {error_type}", endpoint=endpoint)

        self.logger.error(f"Slack API error on {endpoint}: {error_type} (Status: {status_code})")
        raise SlackApiRequestError(
            message=f"Slack API error: {error_type}",
            endpoint=endpoint,
            status_code=status_code,
        )

    # --- Messaging ---

    def post_message(self, channel: str, text: str, blocks: list[dict] | None = None, **kwargs) -> dict[str, Any]:
        """Post a message to a channel."""
        try:
            response = self.client.chat_postMessage(channel=channel, text=text, blocks=blocks, **kwargs)
            return response.data  # type: ignore
        except SlackApiError as e:
            self._handle_error(e, "chat.postMessage")
            raise  # Should not be reached

    def update_message(
        self, channel: str, ts: str, text: str, blocks: list[dict] | None = None, **kwargs
    ) -> dict[str, Any]:
        """Update an existing message."""
        try:
            response = self.client.chat_update(channel=channel, ts=ts, text=text, blocks=blocks, **kwargs)
            return response.data  # type: ignore
        except SlackApiError as e:
            self._handle_error(e, "chat.update")
            raise

    def delete_message(self, channel: str, ts: str, **kwargs) -> dict[str, Any]:
        """Delete a message."""
        try:
            response = self.client.chat_delete(channel=channel, ts=ts, **kwargs)
            return response.data  # type: ignore
        except SlackApiError as e:
            self._handle_error(e, "chat.delete")
            raise

    def post_ephemeral(
        self, channel: str, user: str, text: str, blocks: list[dict] | None = None, **kwargs
    ) -> dict[str, Any]:
        """Post an ephemeral message to a user in a channel."""
        try:
            response = self.client.chat_postEphemeral(channel=channel, user=user, text=text, blocks=blocks, **kwargs)
            return response.data  # type: ignore
        except SlackApiError as e:
            self._handle_error(e, "chat.postEphemeral")
            raise

    # --- Conversations ---

    def get_conversation_history(self, channel: str, limit: int = 100, **kwargs) -> dict[str, Any]:
        """Fetch conversation history."""
        try:
            response = self.client.conversations_history(channel=channel, limit=limit, **kwargs)
            return response.data  # type: ignore
        except SlackApiError as e:
            self._handle_error(e, "conversations.history")
            raise

    def get_conversation_replies(self, channel: str, ts: str, limit: int = 100, **kwargs) -> dict[str, Any]:
        """Fetch replies to a specific thread."""
        try:
            response = self.client.conversations_replies(channel=channel, ts=ts, limit=limit, **kwargs)
            return response.data  # type: ignore
        except SlackApiError as e:
            self._handle_error(e, "conversations.replies")
            raise

    def list_conversations(
        self, types: str = "public_channel,private_channel", limit: int = 100, **kwargs
    ) -> dict[str, Any]:
        """List channels in the workspace."""
        try:
            response = self.client.conversations_list(types=types, limit=limit, **kwargs)
            return response.data  # type: ignore
        except SlackApiError as e:
            self._handle_error(e, "conversations.list")
            raise

    def get_conversation_info(self, channel: str, **kwargs) -> dict[str, Any]:
        """Get info about a conversation."""
        try:
            response = self.client.conversations_info(channel=channel, **kwargs)
            return response.data  # type: ignore
        except SlackApiError as e:
            self._handle_error(e, "conversations.info")
            raise

    # --- Users ---

    def get_user_info(self, user: str, **kwargs) -> dict[str, Any]:
        """Get info about a specific user."""
        try:
            response = self.client.users_info(user=user, **kwargs)
            return response.data  # type: ignore
        except SlackApiError as e:
            self._handle_error(e, "users.info")
            raise

    def list_users(self, limit: int = 100, **kwargs) -> dict[str, Any]:
        """List all users in the workspace."""
        try:
            response = self.client.users_list(limit=limit, **kwargs)
            return response.data  # type: ignore
        except SlackApiError as e:
            self._handle_error(e, "users.list")
            raise

    def lookup_user_by_email(self, email: str) -> dict[str, Any]:
        """Find a user by their email address."""
        try:
            response = self.client.users_lookupByEmail(email=email)
            return response.data  # type: ignore
        except SlackApiError as e:
            self._handle_error(e, "users.lookupByEmail")
            raise

    # --- Files ---

    def upload_file(
        self,
        content: Any = None,
        file: str | None = None,
        filename: str | None = None,
        channels: str | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        """Upload a file to Slack."""
        try:
            response = self.client.files_upload(
                content=content, file=file, filename=filename, channels=channels, **kwargs
            )
            return response.data  # type: ignore
        except SlackApiError as e:
            self._handle_error(e, "files.upload")
            raise

    # --- Search ---

    def search_messages(
        self,
        query: str,
        sort: str = "timestamp",
        sort_dir: str = "desc",
        count: int = 100,
        page: int = 1,
        **kwargs,
    ) -> dict[str, Any]:
        """Search for messages matching a query.

        Args:
            query: Search query string (supports Slack search modifiers like in:#channel).
            sort: Sort field ('timestamp' or 'score').
            sort_dir: Sort direction ('asc' or 'desc').
            count: Number of results per page (max 100).
            page: Page number for pagination.

        Returns:
            Raw API response data containing messages.matches and pagination info.
        """
        try:
            response = self.client.search_messages(
                query=query, sort=sort, sort_dir=sort_dir, count=count, page=page, **kwargs
            )
            return response.data  # type: ignore
        except SlackApiError as e:
            self._handle_error(e, "search.messages")
            raise

    # --- Reactions ---

    def add_reaction(self, channel: str, timestamp: str, name: str) -> dict[str, Any]:
        """Add a reaction to an item."""
        try:
            response = self.client.reactions_add(channel=channel, timestamp=timestamp, name=name)
            return response.data  # type: ignore
        except SlackApiError as e:
            self._handle_error(e, "reactions.add")
            raise

    def get_reactions(self, channel: str, timestamp: str, **kwargs) -> dict[str, Any]:
        """Get reactions for an item."""
        try:
            response = self.client.reactions_get(channel=channel, timestamp=timestamp, **kwargs)
            return response.data  # type: ignore
        except SlackApiError as e:
            self._handle_error(e, "reactions.get")
            raise
