"""
LinearB Performance Metrics Command.
"""

from argparse import ArgumentParser, Namespace

from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_linearb_env_loaded
from utils.logging.logging_manager import LogManager

from .linearb_service import LinearBService


class PerformanceMetricsCommand(BaseCommand):
    """Command to fetch performance metrics from LinearB."""

    @staticmethod
    def get_name() -> str:
        return "performance-metrics"

    @staticmethod
    def get_description() -> str:
        return "Get performance metrics from LinearB for teams and contributors"

    @staticmethod
    def get_help() -> str:
        return """
Get performance metrics from LinearB for teams and contributors.

This command fetches performance data from LinearB including cycle times,
PR metrics, commit metrics, and other team performance indicators.

Examples:
  # Get last week's team performance with daily granularity
  python src/main.py linearb performance-metrics --granularity 1d --time-range last-week

  # Get last month's performance with weekly granularity
  python src/main.py linearb performance-metrics --granularity 1w --time-range last-month

  # Get performance for custom date range (requires granularity=custom)
  python src/main.py linearb performance-metrics --granularity custom \\
    --time-range 2025-07-14,2025-07-20

  # Get contributor-level performance for last 2 weeks
  python src/main.py linearb performance-metrics --granularity 1d \\
    --time-range last-2-weeks --filter-type contributor

  # Get specific team performance
  python src/main.py linearb performance-metrics --granularity custom \\
    --time-range 2025-07-15,2025-07-22 --team-ids 41576

Available team IDs:
  - 19767: Core Services Tribe
  - 41576: Farm Operations Team

Available granularities: 1d, 1w, 1mo, custom
  - 1d, 1w, 1mo: Use with predefined time ranges (last-week, last-month, N-days)
  - custom: Use with specific date ranges (YYYY-MM-DD,YYYY-MM-DD)

Available filter types: organization, contributor, team, repository, label, custom_metric
Available aggregations: p75, avg, p50
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--team-ids",
            type=str,
            help="Comma-separated team IDs to filter by (e.g., '19767,41576')",
        )

        parser.add_argument(
            "--granularity",
            type=str,
            choices=["1d", "1w", "1mo", "custom"],
            default="custom",
            help="Data granularity (default: custom)",
        )

        parser.add_argument(
            "--time-range",
            type=str,
            help=(
                "Time range for metrics. Required when granularity is 'custom'. "
                "For custom: YYYY-MM-DD,YYYY-MM-DD (e.g., '2025-07-14,2025-07-20'). "
                "For other granularities: last-week, last-2-weeks, last-month, N-days"
            ),
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
            default="contributor",
            help="Filter type for grouping data (default: contributor)",
        )

        parser.add_argument(
            "--aggregation",
            type=str,
            choices=["p75", "avg", "p50"],
            default="p75",
            help="Aggregation type for time-based metrics (default: p75)",
        )

        parser.add_argument(
            "--output-file",
            type=str,
            help="Output file path for saving results (optional)",
        )

        parser.add_argument(
            "--save-results",
            action="store_true",
            help="Save results to a JSON file in the output directory",
        )

        parser.add_argument(
            "--show-summary",
            action="store_true",
            help="Show a summary of the performance metrics",
        )

    @staticmethod
    def main(args: Namespace):
        ensure_linearb_env_loaded()
        logger = LogManager.get_instance().get_logger("PerformanceMetricsCommand")

        try:
            # Validate granularity and time-range relationship
            if args.granularity == "custom":
                if not args.time_range:
                    logger.error(
                        "--time-range is required when granularity is 'custom'"
                    )
                    logger.error(
                        "Use format: YYYY-MM-DD,YYYY-MM-DD (e.g., '2025-07-14,2025-07-20')"
                    )
                    exit(1)

                # Validate custom time range format
                if "," not in args.time_range:
                    logger.error(
                        "Custom time range must be in format: YYYY-MM-DD,YYYY-MM-DD"
                    )
                    logger.error(f"Invalid format: {args.time_range}")
                    exit(1)

                # Validate date format
                try:
                    from datetime import datetime

                    start_date, end_date = args.time_range.split(",")
                    datetime.strptime(start_date.strip(), "%Y-%m-%d")
                    datetime.strptime(end_date.strip(), "%Y-%m-%d")
                except ValueError:
                    logger.error(
                        f"Invalid date format in time range: {args.time_range}"
                    )
                    logger.error("Expected format: YYYY-MM-DD,YYYY-MM-DD")
                    exit(1)

                logger.info(
                    f"Using custom granularity with time range: {args.time_range}"
                )

            else:
                # For predefined granularities (1d, 1w, 1mo), use default time range if not provided
                if not args.time_range:
                    # Default to last week for predefined granularities
                    args.time_range = "last-week"
                    logger.info(
                        f"No time range specified, using default: {args.time_range}"
                    )

                # Validate that predefined granularities don't use custom date format
                if "," in args.time_range and len(args.time_range.split(",")) == 2:
                    try:
                        from datetime import datetime

                        start_date, end_date = args.time_range.split(",")
                        datetime.strptime(start_date.strip(), "%Y-%m-%d")
                        datetime.strptime(end_date.strip(), "%Y-%m-%d")
                        # If we reach here, it's a valid date range format
                        logger.error(
                            f"Custom date format '{args.time_range}' requires --granularity custom"
                        )
                        logger.error(
                            "Use predefined time ranges (last-week, last-month, N-days) "
                            "with granularities 1d, 1w, 1mo"
                        )
                        exit(1)
                    except ValueError:
                        # Not a valid date range format, continue with normal processing
                        pass

                logger.info(
                    f"Using {args.granularity} granularity with time range: {args.time_range}"
                )

            logger.info("Initializing LinearB service...")
            service = LinearBService()

            # Test the API connection first
            logger.info("Testing LinearB API connection...")
            connection_test = service.test_connection()

            if connection_test["status"] != "success":
                logger.error(
                    f"LinearB API connection failed: {connection_test['message']}"
                )
                logger.error(f"API key prefix: {connection_test['api_key_prefix']}")
                logger.error("Please verify your LINEARB_API_KEY in the .env file")
                exit(1)

            logger.info("API connection successful. Fetching performance metrics...")

            # Get performance metrics
            metrics_data = service.get_performance_metrics(args)

            # Show summary if requested
            if args.show_summary:
                summary = service.get_team_performance_summary(metrics_data)
                logger.info("Performance Summary:")
                logger.info(f"Total time periods: {summary['total_time_periods']}")
                logger.info(f"Parameters: {summary['parameters']}")

                for i, period_summary in enumerate(summary["teams_summary"]):
                    logger.info(f"Period {i + 1}: {period_summary['period']}")
                    logger.info(f"  Teams: {period_summary['teams_count']}")
                    logger.info(f"  Metrics: {period_summary['metrics_available']}")

            # Save results if requested
            if args.save_results or args.output_file:
                output_path = service.save_metrics_to_file(
                    metrics_data, args.output_file
                )
                logger.info(f"Results saved to: {output_path}")

            # Display key metrics
            total_periods = len(metrics_data.get("metrics", []))
            logger.info(
                f"Successfully retrieved metrics for {total_periods} time period(s)"
            )

            for i, period in enumerate(metrics_data.get("metrics", [])):
                period_label = f"{period.get('after', 'Unknown')} to {period.get('before', 'Unknown')}"
                team_count = len(period.get("metrics", []))
                logger.info(
                    f"Period {i + 1} ({period_label}): {team_count} team(s)/entity(ies)"
                )

            logger.info("Performance metrics retrieval completed successfully")

        except Exception as e:
            logger.error(f"Performance metrics command failed: {e}")
            exit(1)
