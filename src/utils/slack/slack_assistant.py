import hashlib
from typing import Any

from utils.cache_manager.cache_manager import CacheManager
from utils.data.json_manager import JSONManager
from utils.logging.logging_manager import LogManager
from utils.slack.slack_api_client import SlackApiClient
from utils.slack.slack_config import SlackConfig


class SlackAssistant:
    """High-level Slack assistant with caching and common business logic."""

    def __init__(
        self,
        cache_expiration_minutes: int = 60,
        bot_token: str | None = None,
        webhook_url: str | None = None,
        default_channel: str | None = None,
    ):
        """Initialize the Slack Assistant.

        Args:
            cache_expiration_minutes: Default expiration for cached data.
            bot_token: Optional Slack bot token.
            webhook_url: Optional Slack webhook URL.
            default_channel: Optional default Slack channel ID.
        """
        self.logger = LogManager.get_instance().get_logger("SlackAssistant")
        self.config = SlackConfig(bot_token=bot_token, webhook_url=webhook_url, default_channel=default_channel)
        self.client = SlackApiClient(self.config.bot_token)
        self.cache_manager = CacheManager.get_instance()
        self.cache_expiration = cache_expiration_minutes

    def _generate_cache_key(self, prefix: str, **kwargs) -> str:
        """Generates a deterministic cache key."""
        sorted_items = JSONManager.create_json(kwargs)
        hash_hex = hashlib.sha256(sorted_items.encode("utf-8")).hexdigest()
        return f"slack_{prefix}_{hash_hex}"

    def _load_from_cache(self, cache_key: str) -> dict | list | None:
        """Load data from cache."""
        try:
            return self.cache_manager.load(cache_key, expiration_minutes=self.cache_expiration)
        except Exception as e:
            self.logger.debug(f"Cache miss or load failure for key '{cache_key}': {e}")
            return None

    def _save_to_cache(self, cache_key: str, data: dict | list):
        """Save data to cache."""
        try:
            self.cache_manager.save(cache_key, data)
        except Exception as e:
            self.logger.warning(f"Failed to cache data for key '{cache_key}': {e}")

    # --- Cached Methods ---

    def get_user_info_cached(self, user_id: str) -> dict[str, Any]:
        """Get user info with caching."""
        cache_key = self._generate_cache_key("user_info", user_id=user_id)
        cached = self._load_from_cache(cache_key)
        if isinstance(cached, dict):
            return cached

        data = self.client.get_user_info(user=user_id)
        self._save_to_cache(cache_key, data)
        return data

    def get_conversation_info_cached(self, channel_id: str) -> dict[str, Any]:
        """Get conversation info with caching."""
        cache_key = self._generate_cache_key("conv_info", channel_id=channel_id)
        cached = self._load_from_cache(cache_key)
        if isinstance(cached, dict):
            return cached

        data = self.client.get_conversation_info(channel=channel_id)
        self._save_to_cache(cache_key, data)
        return data

    def get_thread_messages(self, channel_id: str, thread_ts: str, use_cache: bool = True) -> list[dict[str, Any]]:
        """Fetch all messages in a thread with optional caching and pagination."""
        cache_key = self._generate_cache_key("thread", channel_id=channel_id, thread_ts=thread_ts)

        if use_cache:
            cached = self._load_from_cache(cache_key)
            if isinstance(cached, list):
                return cached

        all_messages: list[dict[str, Any]] = []
        cursor: str | None = None

        while True:
            response = self.client.get_conversation_replies(channel=channel_id, ts=thread_ts, cursor=cursor, limit=100)

            messages = response.get("messages", [])
            all_messages.extend(messages)

            cursor = response.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break

        if use_cache:
            self._save_to_cache(cache_key, all_messages)

        return all_messages

    # --- Non-cached Methods ---

    def search_channel_messages(
        self,
        channel_name: str,
        query: str,
        sort: str = "timestamp",
        use_cache: bool = True,
    ) -> list[dict[str, Any]]:
        """Search for messages in a specific channel with pagination and caching.

        Uses the search.messages API to find messages matching a query within
        a specific channel. Handles pagination automatically (max 100 pages x 100 results).

        Args:
            channel_name: Channel name without '#' prefix (e.g., 'global-kudos').
            query: Search query to combine with channel filter.
            sort: Sort field ('timestamp' or 'score').
            use_cache: Whether to use caching for results.

        Returns:
            List of message match dicts from the search.messages API.
        """
        full_query = f"in:#{channel_name} {query}"
        cache_key = self._generate_cache_key("search_messages", query=full_query, sort=sort)

        if use_cache:
            cached = self._load_from_cache(cache_key)
            if isinstance(cached, list):
                self.logger.info(f"Cache hit for search query: {full_query} ({len(cached)} results)")
                return cached

        all_matches: list[dict[str, Any]] = []
        page = 1
        max_pages = 100

        while page <= max_pages:
            response = self.client.search_messages(
                query=full_query,
                sort=sort,
                sort_dir="asc",
                count=100,
                page=page,
            )

            messages = response.get("messages", {})
            matches = messages.get("matches", [])
            all_matches.extend(matches)

            total = messages.get("total", 0)
            pagination = messages.get("pagination", {})
            page_count = pagination.get("page_count", 1)

            self.logger.debug(f"Search page {page}/{page_count}: {len(matches)} matches (total: {total})")

            if page >= page_count:
                break
            page += 1

        self.logger.info(f"Search completed for '{full_query}': {len(all_matches)} total matches")

        if use_cache:
            self._save_to_cache(cache_key, all_matches)

        return all_matches

    def send_message(
        self, channel_id: str | None = None, text: str = "", blocks: list[dict] | None = None, **kwargs
    ) -> dict[str, Any]:
        """Send a message, using default channel if none provided."""
        channel = channel_id or self.config.default_channel
        if not channel:
            raise ValueError("No channel ID provided and no default channel configured.")

        return self.client.post_message(channel=channel, text=text, blocks=blocks, **kwargs)

    def send_notification(self, title: str, message: str, channel_id: str | None = None) -> dict[str, Any]:
        """High-level helper to send a formatted notification."""
        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": title}},
            {"type": "section", "text": {"type": "mrkdwn", "text": message}},
        ]
        return self.send_message(channel_id=channel_id, text=f"{title}: {message}", blocks=blocks)
