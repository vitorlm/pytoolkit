import re
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from domains.syngenta.team_assessment.core.kudos import Kudos, MemberKudos
from utils.data.json_manager import JSONManager
from utils.logging.logging_manager import LogManager
from utils.slack.slack_assistant import SlackAssistant


class KudosService:
    """Fetches and parses kudos from the Slack #global-kudos channel.

    Uses the search.messages API to find Appreci bot messages mentioning each
    team member, then parses them to extract sender, message, and metadata.

    Args:
        mapping_file: Path to the member→Slack User ID mapping JSON file.
        channel: Slack channel name to search in (without # prefix).
        year: Assessment year to filter kudos by.
    """

    # Pattern for Appreci bot context block text:
    # "<@U...>,<@U...> just received a kudos from Sender Name :point_down:"
    APPRECI_PATTERN = re.compile(
        r"just received a kudos from\s+(.+?)(?:\s*:\w+:|$)",
        re.IGNORECASE,
    )

    def __init__(self, mapping_file: str, channel: str, year: int, bot_token: str) -> None:
        self.logger = LogManager.get_instance().get_logger("KudosService")
        self.channel = channel
        self.year = year

        # Load member → Slack User ID mapping
        self.member_slack_mapping: dict[str, str] = self._load_mapping(mapping_file)

        # Initialize Slack assistant using the provided bot token
        if not bot_token:
            raise ValueError("Slack bot token is required for kudos integration")

        self.slack_assistant = SlackAssistant(bot_token=bot_token, cache_expiration_minutes=120)
        self.logger.info(
            f"KudosService initialized: channel=#{channel}, year={year}, "
            f"mapped_members={len(self.member_slack_mapping)}"
        )

    def _load_mapping(self, mapping_file: str) -> dict[str, str]:
        """Loads the member name → Slack User ID mapping from a JSON file.

        Args:
            mapping_file: Path to the JSON mapping file.

        Returns:
            Dictionary mapping member full names to Slack User IDs.
        """
        path = Path(mapping_file)
        if not path.exists():
            self.logger.warning(f"Member-Slack mapping file not found: {mapping_file}")
            return {}

        data = JSONManager.read_json(mapping_file)
        if not isinstance(data, dict):
            self.logger.error(f"Invalid mapping file format (expected dict): {mapping_file}")
            return {}

        self.logger.info(f"Loaded {len(data)} member-Slack mappings from {mapping_file}")
        return data

    def fetch_kudos_for_member(self, member_name: str) -> MemberKudos:
        """Fetches all kudos received by a specific member from the Slack channel.

        Args:
            member_name: Full member name as it appears in feedback files.

        Returns:
            MemberKudos with all parsed kudos for the member.
        """
        slack_user_id = self._resolve_slack_user_id(member_name)
        if not slack_user_id:
            return MemberKudos(total_count=0, kudos=[], senders=[], period=str(self.year))

        self.logger.info(f"Fetching kudos for {member_name} (Slack ID: {slack_user_id})")

        # Search for messages mentioning this user in the kudos channel
        query = f"<@{slack_user_id}>"
        matches = self.slack_assistant.search_channel_messages(
            channel_name=self.channel,
            query=query,
            sort="timestamp",
        )

        # Parse and filter matches
        kudos_list: list[Kudos] = []
        for match in matches:
            kudos = self._parse_appreci_message(match)
            if kudos and self._is_in_year(kudos.timestamp):
                kudos_list.append(kudos)

        # Build unique senders list
        senders = list({k.sender for k in kudos_list})

        member_kudos = MemberKudos(
            total_count=len(kudos_list),
            kudos=kudos_list,
            senders=senders,
            period=str(self.year),
        )

        self.logger.info(
            f"Found {member_kudos.total_count} kudos for {member_name} "
            f"from {len(senders)} unique senders in {self.year}"
        )
        return member_kudos

    def fetch_kudos_for_all_members(self, member_names: list[str]) -> dict[str, MemberKudos]:
        """Fetches kudos for all team members.

        Args:
            member_names: List of full member names.

        Returns:
            Dictionary mapping member names to their MemberKudos data.
        """
        self.logger.info(f"Fetching kudos for {len(member_names)} members from #{self.channel}")
        results: dict[str, MemberKudos] = {}

        for member_name in member_names:
            try:
                results[member_name] = self.fetch_kudos_for_member(member_name)
            except Exception as e:
                self.logger.error(f"Failed to fetch kudos for {member_name}: {e}", exc_info=True)
                results[member_name] = MemberKudos(total_count=0, kudos=[], senders=[], period=str(self.year))

        total_kudos = sum(mk.total_count for mk in results.values())
        self.logger.info(f"Kudos fetch complete: {total_kudos} total kudos across {len(results)} members")
        return results

    def _parse_appreci_message(self, message: dict) -> Kudos | None:
        """Parses an Appreci bot message to extract kudos data.

        Appreci bot messages use Slack blocks with the following structure:
        - Block type "context": contains mrkdwn text with sender and recipients
          Pattern: "<@U...> just received a kudos from Sender Name :point_down:"
        - Block type "image": image_url contains query params kContent (message)
          and kValue (kudos category like "Amazing Job", "Team Player")

        Non-Appreci messages (e.g., thread replies from users) are skipped.

        Args:
            message: A search result match dict from the Slack API.

        Returns:
            Kudos object if the message is from Appreci bot, None otherwise.
        """
        username = message.get("username", "")
        if username != "appreci bot":
            return None

        ts = message.get("ts", "")
        permalink = message.get("permalink", "")
        blocks = message.get("blocks", [])

        # Extract sender from context block
        sender_name = ""
        context_block = next((b for b in blocks if b.get("type") == "context"), None)
        if context_block:
            elements = context_block.get("elements", [])
            for element in elements:
                text = element.get("text", "")
                match = self.APPRECI_PATTERN.search(text)
                if match:
                    sender_name = match.group(1).strip()
                    break

        if not sender_name:
            self.logger.debug(f"Could not parse sender from Appreci message ts={ts}")
            return None

        # Extract kudos content and value from image block URL
        kudos_message, kudos_value = self._extract_kudos_content_from_blocks(blocks)

        # Prepend the kudos value/category if available (e.g., "[Amazing Job] ...")
        full_message = f"[{kudos_value}] {kudos_message}" if kudos_value else kudos_message

        return Kudos(
            sender=sender_name,
            message=full_message,
            timestamp=ts,
            permalink=permalink,
        )

    def _extract_kudos_content_from_blocks(self, blocks: list[dict]) -> tuple[str, str]:
        """Extracts kudos content and value from Appreci bot image block URL.

        The image block URL contains query parameters:
        - kContent: The free-text kudos message
        - kValue: The kudos category (e.g., "Amazing Job", "Team Player", "Impressive")

        Args:
            blocks: List of Slack block dicts from the message.

        Returns:
            Tuple of (content_text, value_category). Both empty strings if not found.
        """
        image_block = next((b for b in blocks if b.get("type") == "image"), None)
        if not image_block:
            return "", ""

        image_url = image_block.get("image_url", "")
        if not image_url:
            return "", ""

        try:
            params = parse_qs(urlparse(image_url).query)
            content = unquote(params.get("kContent", [""])[0])
            value = unquote(params.get("kValue", [""])[0])
            return content.strip(), value.strip()
        except Exception as e:
            self.logger.debug(f"Failed to parse image URL params: {e}")
            return "", ""

    def _resolve_slack_user_id(self, member_name: str) -> str | None:
        """Looks up the Slack User ID for a member from the mapping.

        Args:
            member_name: Full member name as it appears in feedback files.

        Returns:
            Slack User ID if found, None otherwise.
        """
        # Try exact match first
        if member_name in self.member_slack_mapping:
            return self.member_slack_mapping[member_name]

        # Try case-insensitive match
        for mapped_name, user_id in self.member_slack_mapping.items():
            if mapped_name.lower() == member_name.lower():
                return user_id

        self.logger.warning(f"Member '{member_name}' not found in Slack mapping — skipping kudos fetch")
        return None

    def _is_in_year(self, ts: str) -> bool:
        """Checks if a Slack message timestamp falls within the assessment year.

        Args:
            ts: Slack message timestamp (Unix epoch as string, e.g., '1704067200.000000').

        Returns:
            True if the message is from the assessment year.
        """
        try:
            msg_time = datetime.fromtimestamp(float(ts), tz=UTC)
            return msg_time.year == self.year
        except (ValueError, OSError):
            self.logger.debug(f"Could not parse timestamp: {ts}")
            return False
