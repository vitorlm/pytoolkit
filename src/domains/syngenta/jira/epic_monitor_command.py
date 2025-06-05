from argparse import ArgumentParser, Namespace
import os
from utils.command.base_command import BaseCommand
from domains.syngenta.jira.epic_monitor_service import EpicCronService
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager


class EpicMonitorCommand(BaseCommand):
    """Command to run epic monitoring and send notifications."""

    @staticmethod
    def get_name() -> str:
        return "epic-monitor"

    @staticmethod
    def get_description() -> str:
        return "Monitor JIRA epics for problems and send Slack notifications."

    @staticmethod
    def get_help() -> str:
        return (
            "This command monitors Catalog squad epics for various problems "
            "and sends notifications to Slack."
        )

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--slack-webhook",
            type=str,
            required=False,
            default=None,  # We'll handle the default in main()
            help="Slack webhook URL for notifications (defaults to SLACK_WEBHOOK_URL env var)",
        )

    @staticmethod
    def main(args: Namespace):
        # Ensure environment variables are loaded
        ensure_env_loaded()

        # Get slack webhook from args or environment variable
        slack_webhook = args.slack_webhook or os.getenv("SLACK_WEBHOOK_URL")

        # Initialize and run the epic monitoring service
        epic_service = EpicCronService(slack_webhook)
        success = epic_service.run_epic_check()

        # Get logger instance for this method
        logger = LogManager.get_instance().get_logger("EpicMonitorCommand")

        if success:
            logger.info("Epic monitoring completed successfully")
        else:
            logger.error("Epic monitoring completed with errors - check logs")
            exit(1)
