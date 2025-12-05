import re
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

from utils.logging.logging_manager import LogManager


class WhatsAppMessage:
    def __init__(self, timestamp: str, sender: str, content: str, raw_line: str):
        self.timestamp = timestamp
        self.sender = sender
        self.content = content
        self.raw_line = raw_line
        self.parsed_datetime = self._parse_datetime()

    def _parse_datetime(self) -> datetime | None:
        try:
            return datetime.strptime(self.timestamp, "[%d/%m/%y, %H:%M:%S]")
        except ValueError:
            return None

    def contains_url(self) -> bool:
        return bool(re.search(r"https?://", self.content))

    def extract_urls(self) -> list[str]:
        url_pattern = r"https?://[^\s]+"
        return re.findall(url_pattern, self.content)

    def get_domains(self) -> list[str]:
        urls = self.extract_urls()
        domains = []
        for url in urls:
            try:
                parsed = urlparse(url)
                if parsed.netloc:
                    domains.append(parsed.netloc.lower())
            except Exception:
                continue
        return domains

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "sender": self.sender,
            "content": self.content,
            "parsed_datetime": self.parsed_datetime.isoformat() if self.parsed_datetime else None,
            "urls": self.extract_urls(),
            "domains": self.get_domains(),
        }


class WhatsAppPatternService:
    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("WhatsAppPatternService")

    def parse_chat_file(self, file_path: str) -> list[WhatsAppMessage]:
        """Parse WhatsApp chat export file into structured messages"""
        messages = []

        try:
            with open(file_path, encoding="utf-8") as file:
                content = file.read()
        except UnicodeDecodeError:
            with open(file_path, encoding="utf-8", errors="ignore") as file:
                content = file.read()

        lines = content.split("\n")
        current_message = None

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Check if line starts with timestamp pattern [DD/MM/YY, HH:MM:SS]
            timestamp_match = re.match(r"(\[\d{2}/\d{2}/\d{2}, \d{2}:\d{2}:\d{2}\])", line)

            if timestamp_match:
                # Save previous message if exists
                if current_message:
                    messages.append(current_message)

                # Parse new message
                timestamp = timestamp_match.group(1)
                remaining = line[len(timestamp) :].strip()

                # Extract sender and content
                if ":" in remaining:
                    sender_part, content = remaining.split(":", 1)
                    sender = sender_part.strip()
                    content = content.strip()
                else:
                    sender = "System"
                    content = remaining

                current_message = WhatsAppMessage(timestamp, sender, content, line)
            # Continuation of previous message
            elif current_message:
                current_message.content += " " + line
                current_message.raw_line += "\n" + line

        # Add last message
        if current_message:
            messages.append(current_message)

        self.logger.info(f"Parsed {len(messages)} messages from {file_path}")
        return messages

    def find_messages_with_domain(self, messages: list[WhatsAppMessage], domain: str) -> list[WhatsAppMessage]:
        """Find messages containing links from a specific domain"""
        domain = domain.lower()
        matching_messages = []

        for message in messages:
            domains = message.get_domains()
            if any(domain in d for d in domains):
                matching_messages.append(message)

        self.logger.info(f"Found {len(matching_messages)} messages with domain '{domain}'")
        return matching_messages

    def find_messages_with_pattern(
        self, messages: list[WhatsAppMessage], pattern: str, regex: bool = False
    ) -> list[WhatsAppMessage]:
        """Find messages matching a text pattern"""
        matching_messages = []

        if regex:
            try:
                compiled_pattern = re.compile(pattern, re.IGNORECASE)
            except re.error as e:
                self.logger.error(f"Invalid regex pattern: {e}")
                return []

        for message in messages:
            if regex:
                if compiled_pattern.search(message.content):
                    matching_messages.append(message)
            elif pattern.lower() in message.content.lower():
                matching_messages.append(message)

        self.logger.info(f"Found {len(matching_messages)} messages matching pattern '{pattern}'")
        return matching_messages

    def find_all_urls(self, messages: list[WhatsAppMessage]) -> list[dict[str, Any]]:
        """Extract all URLs from messages"""
        url_data = []

        for message in messages:
            urls = message.extract_urls()
            for url in urls:
                url_data.append(
                    {
                        "url": url,
                        "domain": urlparse(url).netloc.lower() if urlparse(url).netloc else "unknown",
                        "timestamp": message.timestamp,
                        "sender": message.sender,
                        "message_content": message.content[:100] + "..."
                        if len(message.content) > 100
                        else message.content,
                    }
                )

        self.logger.info(f"Extracted {len(url_data)} URLs from messages")
        return url_data

    def get_domain_statistics(self, messages: list[WhatsAppMessage]) -> dict[str, int]:
        """Get statistics of domains mentioned in messages"""
        domain_counts = {}

        for message in messages:
            domains = message.get_domains()
            for domain in domains:
                domain_counts[domain] = domain_counts.get(domain, 0) + 1

        # Sort by count descending
        sorted_domains = dict(sorted(domain_counts.items(), key=lambda x: x[1], reverse=True))

        self.logger.info(f"Found {len(sorted_domains)} unique domains")
        return sorted_domains

    def find_messages_by_date_range(
        self, messages: list[WhatsAppMessage], start_date: str, end_date: str
    ) -> list[WhatsAppMessage]:
        """Find messages within a date range (format: DD/MM/YY)"""
        try:
            start_dt = datetime.strptime(start_date, "%d/%m/%y")
            end_dt = datetime.strptime(end_date, "%d/%m/%y")
        except ValueError as e:
            self.logger.error(f"Invalid date format: {e}")
            return []

        matching_messages = []
        for message in messages:
            if message.parsed_datetime:
                msg_date = message.parsed_datetime.date()
                if start_dt.date() <= msg_date <= end_dt.date():
                    matching_messages.append(message)

        self.logger.info(f"Found {len(matching_messages)} messages between {start_date} and {end_date}")
        return matching_messages

    def extract_portal_sped_urls(self, file_path: str) -> list[dict[str, Any]]:
        """Extract Portal SPED URLs from WhatsApp export file

        Args:
            file_path: Path to WhatsApp export file

        Returns:
            List of dictionaries containing Portal SPED URL data
        """
        self.logger.info(f"Extracting Portal SPED URLs from: {file_path}")

        # Parse messages from file
        messages = self.parse_chat_file(file_path)

        # Find messages with Portal SPED domain
        portal_sped_messages = self.find_messages_with_domain(messages, "portalsped.fazenda.mg.gov.br")

        # Extract Portal SPED URLs
        portal_sped_urls = []
        for message in portal_sped_messages:
            urls = message.extract_urls()
            for url in urls:
                if "portalsped.fazenda.mg.gov.br" in url.lower():
                    portal_sped_urls.append(
                        {
                            "url": url,
                            "timestamp": message.timestamp,
                            "sender": message.sender,
                            "message_content": message.content[:200] + "..."
                            if len(message.content) > 200
                            else message.content,
                            "parsed_datetime": message.parsed_datetime.isoformat() if message.parsed_datetime else None,
                        }
                    )

        self.logger.info(f"Extracted {len(portal_sped_urls)} Portal SPED URLs")
        return portal_sped_urls
