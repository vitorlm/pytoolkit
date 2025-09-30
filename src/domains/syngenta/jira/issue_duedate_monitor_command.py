from argparse import ArgumentParser, Namespace
import os
from utils.command.base_command import BaseCommand
from domains.syngenta.jira.issue_duedate_monitor_service import (
    IssueDueDateMonitorService,
)
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager


class IssueDueDateMonitorCommand(BaseCommand):
    """Command to monitor JIRA issues without due dates and send notifications."""

    @staticmethod
    def get_name() -> str:
        return "issue-duedate-monitor"

    @staticmethod
    def get_description() -> str:
        return "Monitor JIRA issues in progress without due dates and send Slack notifications."

    @staticmethod
    def get_help() -> str:
        return (
            "This command monitors issues from specified squads that are in progress "
            "but don't have due dates assigned, sending notifications to Slack when found."
        )

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--squad",
            type=str,
            required=True,
            help="Squad name to monitor (e.g., 'Catalog', 'Recommendations')",
        )
        parser.add_argument(
            "--project-key",
            type=str,
            required=False,
            default="CWS",
            help="JIRA project key (defaults to 'CWS')",
        )
        parser.add_argument(
            "--issue-types",
            type=str,
            required=False,
            default="Bug,Support,Story,Task,Technical Debt,Improvement,Defect",
            help="Comma-separated list of issue types to monitor (e.g., 'Story,Task,Bug'). If not specified, all types are monitored.",
        )
        parser.add_argument(
            "--slack-webhook",
            type=str,
            required=False,
            default=None,
            help="Slack webhook URL for notifications (defaults to SLACK_WEBHOOK_URL env var)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Run without sending notifications (just log findings)",
        )

    @staticmethod
    def main(args: Namespace):
        # Ensure environment variables are loaded
        ensure_env_loaded()

        # Get logger instance for this method
        logger = LogManager.get_instance().get_logger("IssueDueDateMonitorCommand")

        try:
            # Get slack webhook from args or environment variable
            slack_webhook = args.slack_webhook or os.getenv("SLACK_WEBHOOK_URL")

            # Initialize and run the issue monitoring service
            monitor_service = IssueDueDateMonitorService(slack_webhook)

            # Parse issue types if provided
            issue_types = None
            if args.issue_types:
                issue_types = [t.strip() for t in args.issue_types.split(",")]

            success = monitor_service.run_issue_check(
                squad=args.squad,
                project_key=args.project_key,
                issue_types=issue_types,
                dry_run=args.dry_run,
            )

            if success:
                logger.info("Issue due date monitoring completed successfully")
                exit(0)
            else:
                logger.error("Issue due date monitoring failed")
                exit(1)

        except Exception as e:
            logger.error(f"Failed to run issue due date monitoring: {e}", exc_info=True)
            exit(1)
