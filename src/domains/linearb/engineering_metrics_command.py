"""
LinearB Engineering Metrics Command.
"""

import sys
from argparse import ArgumentParser, Namespace

from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_linearb_env_loaded
from utils.logging.logging_manager import LogManager

from .linearb_service import LinearBService


class EngineeringMetricsCommand(BaseCommand):
    """Command to fetch engineering metrics from LinearB."""

    @staticmethod
    def get_name() -> str:
        return "engineering-metrics"

    @staticmethod
    def get_description() -> str:
        return "Get engineering metrics from LinearB for software engineering analysis"

    @staticmethod
    def get_help() -> str:
        return """
Get engineering metrics from LinearB for software engineering analysis.

This command fetches specific engineering metrics including:
- PRs Merged Without Review
- Review Depth
- PR Maturity
- Deploy Time
- PR Size
- Deploy Frequency
- Cycle Time
- Pickup Time
- Review Time

Examples:
  # Get last week's engineering metrics with daily granularity (CSV output)
  python src/main.py linearb engineering-metrics --time-range last-week --granularity 1d

  # Get last month's engineering metrics with weekly granularity (JSON output)
  python src/main.py linearb engineering-metrics --time-range last-month \\
    --granularity 1w --format json

  # Get engineering metrics for specific teams
  python src/main.py linearb engineering-metrics --time-range last-2-weeks \\
    --granularity 1d --team-ids 123,456,789

  # Get engineering metrics for custom date range with specific aggregation
  python src/main.py linearb engineering-metrics \\
    --time-range 2025-07-01,2025-07-07 --granularity custom \\
    --aggregation p75 --filter-type team

  # Get contributor-level engineering metrics
  python src/main.py linearb engineering-metrics --time-range last-week \\
    --granularity 1d --filter-type contributor

  # Save to custom output folder
  python src/main.py linearb engineering-metrics --time-range last-week \\
    --output-folder reports/engineering
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        """Add command-specific arguments."""
        # Required arguments
        parser.add_argument(
            "--time-range",
            required=True,
            type=str,
            help="Time range: last-week, last-2-weeks, last-month, "
            "N-days (e.g., 7-days), or date range YYYY-MM-DD,YYYY-MM-DD",
        )

        # Optional arguments
        parser.add_argument(
            "--team-ids",
            type=str,
            help="Comma-separated list of team IDs to filter by (e.g., 123,456,789)",
        )

        parser.add_argument(
            "--granularity",
            type=str,
            choices=["1d", "1w", "1mo", "custom"],
            default="1d",
            help="Time granularity: 1d (daily), 1w (weekly), 1mo (monthly), custom (default)",
        )

        parser.add_argument(
            "--filter-type",
            type=str,
            choices=[
                "organization",
                "contributor",
                "team",
                "repository",
                "label",
                "custom_metric",
            ],
            default="team",
            help="Filter type: organization, contributor, team (default), "
            "repository, label, custom_metric",
        )

        parser.add_argument(
            "--aggregation",
            type=str,
            choices=["p75", "avg", "p50", "raw", "default"],
            default="default",
            help="Aggregation type: p75, avg, p50, raw, default (no aggregation)",
        )

        parser.add_argument(
            "--format",
            type=str,
            choices=["csv", "json"],
            default="csv",
            help="Output format: csv (default) or json",
        )

        parser.add_argument(
            "--output-folder",
            type=str,
            default="output",
            help="Folder where the report will be saved (default: output)",
        )

    @staticmethod
    def main(args: Namespace):
        """Execute the engineering metrics command."""
        ensure_linearb_env_loaded()
        logger = LogManager.get_instance().get_logger("EngineeringMetricsCommand")

        try:
            service = LinearBService()
            service.get_engineering_metrics(args)

            logger.info("Engineering metrics command completed successfully")

        except Exception as e:
            logger.error(f"Engineering metrics command failed: {e}")
            print(f"Error: {e}")
            sys.exit(1)
