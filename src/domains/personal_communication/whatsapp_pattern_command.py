from argparse import ArgumentParser, Namespace
from typing import Any

from domains.personal_communication.whatsapp_pattern_service import (
    WhatsAppMessage,
    WhatsAppPatternService,
)
from utils.command.base_command import BaseCommand
from utils.data.json_manager import JSONManager
from utils.env_loader import ensure_env_loaded
from utils.file_manager import FileManager
from utils.logging.logging_manager import LogManager


class WhatsAppPatternCommand(BaseCommand):
    @staticmethod
    def get_name() -> str:
        return "whatsapp-pattern"

    @staticmethod
    def get_description() -> str:
        return "Find patterns in WhatsApp chat exports (links, domains, text patterns)"

    @staticmethod
    def get_help() -> str:
        return """
WhatsApp Pattern Finder

This command analyzes WhatsApp chat export files to find specific patterns like:
- Links from specific domains
- Text patterns (with regex support)
- URL statistics and domain analysis
- Messages within date ranges

Usage Examples:
  # Find all messages with YouTube links
  python src/main.py personal-communication whatsapp-pattern --file _chat.txt --domain youtube.com

  # Find messages containing specific text
  python src/main.py personal-communication whatsapp-pattern --file _chat.txt --pattern "github"

  # Find messages with regex pattern
  python src/main.py personal-communication whatsapp-pattern --file _chat.txt --pattern "\\d{10,}" --regex

  # Get domain statistics
  python src/main.py personal-communication whatsapp-pattern --file _chat.txt --stats

  # Find all URLs
  python src/main.py personal-communication whatsapp-pattern --file _chat.txt --all-urls

  # Find messages in date range
  python src/main.py personal-communication whatsapp-pattern --file _chat.txt --start-date "01/01/23" --end-date "31/12/23"

  # Export results to JSON
  python src/main.py personal-communication whatsapp-pattern --file _chat.txt --domain github.com --output results.json

File format: WhatsApp chat export (.txt format)
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument("--file", required=True, help="Path to WhatsApp chat export file (.txt)")

        # Search options (mutually exclusive group)
        search_group = parser.add_mutually_exclusive_group()
        search_group.add_argument("--domain", help="Find messages with links from specific domain")
        search_group.add_argument("--pattern", help="Find messages containing text pattern")
        search_group.add_argument("--stats", action="store_true", help="Show domain statistics")
        search_group.add_argument("--all-urls", action="store_true", help="Extract all URLs from messages")

        # Additional options
        parser.add_argument(
            "--regex",
            action="store_true",
            help="Treat pattern as regex (use with --pattern)",
        )
        parser.add_argument("--start-date", help="Start date for filtering (format: DD/MM/YY)")
        parser.add_argument("--end-date", help="End date for filtering (format: DD/MM/YY)")
        parser.add_argument("--output", help="Output file path for results (JSON format)")
        parser.add_argument(
            "--limit",
            type=int,
            default=50,
            help="Limit number of results shown (default: 50)",
        )
        parser.add_argument(
            "--include-content",
            action="store_true",
            help="Include full message content in output",
        )

    @staticmethod
    def main(args: Namespace):
        ensure_env_loaded()
        logger = LogManager.get_instance().get_logger("WhatsAppPatternCommand")

        try:
            # Validate file
            FileManager.validate_file(args.file, allowed_extensions=[".txt"])

            # Initialize service
            service = WhatsAppPatternService()

            # Parse messages
            logger.info(f"Parsing WhatsApp chat file: {args.file}")
            messages = service.parse_chat_file(args.file)

            if not messages:
                logger.warning("No messages found in the file")
                return

            logger.info(f"Loaded {len(messages)} messages")

            # Apply date filtering if specified
            if args.start_date and args.end_date:
                messages = service.find_messages_by_date_range(messages, args.start_date, args.end_date)
                logger.info(f"Filtered to {len(messages)} messages in date range")

            # Execute the requested operation
            results = []

            if args.domain:
                matching_messages = service.find_messages_with_domain(messages, args.domain)
                results = WhatsAppPatternCommand._format_messages(matching_messages, args.include_content, args.limit)
                print(f"\nFound {len(matching_messages)} messages with domain '{args.domain}':")

            elif args.pattern:
                matching_messages = service.find_messages_with_pattern(messages, args.pattern, args.regex)
                results = WhatsAppPatternCommand._format_messages(matching_messages, args.include_content, args.limit)
                pattern_type = "regex" if args.regex else "text"
                print(f"\nFound {len(matching_messages)} messages matching {pattern_type} pattern '{args.pattern}':")

            elif args.stats:
                domain_stats = service.get_domain_statistics(messages)
                results = {
                    "domain_statistics": domain_stats,
                    "total_domains": len(domain_stats),
                }
                print(f"\nDomain Statistics ({len(domain_stats)} unique domains):")
                for domain, count in list(domain_stats.items())[: args.limit]:
                    print(f"  {domain}: {count} messages")
                if len(domain_stats) > args.limit:
                    print(f"  ... and {len(domain_stats) - args.limit} more domains")

            elif args.all_urls:
                url_data = service.find_all_urls(messages)
                results = {"urls": url_data[: args.limit], "total_urls": len(url_data)}
                print(f"\nFound {len(url_data)} URLs:")
                for url_info in url_data[: args.limit]:
                    print(f"  {url_info['timestamp']} - {url_info['domain']}")
                    print(f"    {url_info['url']}")
                if len(url_data) > args.limit:
                    print(f"  ... and {len(url_data) - args.limit} more URLs")

            else:
                # No specific operation, show summary
                total_with_urls = len([m for m in messages if m.contains_url()])
                domain_stats = service.get_domain_statistics(messages)

                print("\nWhatsApp Chat Summary:")
                print(f"  Total messages: {len(messages)}")
                print(f"  Messages with URLs: {total_with_urls}")
                print(f"  Unique domains: {len(domain_stats)}")

                if domain_stats:
                    print("\nTop domains:")
                    for domain, count in list(domain_stats.items())[:10]:
                        print(f"    {domain}: {count}")

                results = {
                    "summary": {
                        "total_messages": len(messages),
                        "messages_with_urls": total_with_urls,
                        "unique_domains": len(domain_stats),
                        "top_domains": dict(list(domain_stats.items())[:10]),
                    }
                }

            # Save results if output file specified
            if args.output and results:
                JSONManager.write_json(results, args.output)
                logger.info(f"Results saved to {args.output}")

            # Display limited results
            if isinstance(results, list) and results:
                WhatsAppPatternCommand._display_messages(results[: args.limit])
                if len(results) > args.limit:
                    print(f"\n... showing {args.limit} of {len(results)} results (use --limit to see more)")

        except Exception as e:
            logger.error(f"Command failed: {e}")
            exit(1)

    @staticmethod
    def _format_messages(messages: list[WhatsAppMessage], include_content: bool, limit: int) -> list[dict[str, Any]]:
        """Format messages for output"""
        results = []
        for message in messages:
            result = {
                "timestamp": message.timestamp,
                "sender": message.sender,
                "urls": message.extract_urls(),
                "domains": message.get_domains(),
            }

            if include_content:
                result["content"] = message.content
            else:
                # Truncate content for display
                content_preview = message.content[:100]
                if len(message.content) > 100:
                    content_preview += "..."
                result["content_preview"] = content_preview

            results.append(result)

        return results

    @staticmethod
    def _display_messages(results: list[dict[str, Any]]):
        """Display formatted messages"""
        for i, result in enumerate(results, 1):
            print(f"\n{i}. {result['timestamp']} - {result['sender']}")

            if "content_preview" in result:
                print(f"   {result['content_preview']}")
            elif "content" in result:
                print(f"   {result['content']}")

            if result.get("urls"):
                for url in result["urls"]:
                    print(f"   🔗 {url}")

            if result.get("domains"):
                print(f"   📍 Domains: {', '.join(result['domains'])}")
