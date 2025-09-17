"""
LinearB Knowledge Sharing Metrics Command.
"""

from argparse import ArgumentParser, Namespace

from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_linearb_env_loaded
from utils.logging.logging_manager import LogManager

from .knowledge_sharing_metrics_service import KnowledgeSharingMetricsService


class KnowledgeSharingMetricsCommand(BaseCommand):
    """Command to analyze PR review patterns and knowledge distribution metrics."""

    @staticmethod
    def get_name() -> str:
        return "knowledge-sharing-metrics"

    @staticmethod
    def get_description() -> str:
        return "Analyze PR review patterns and knowledge distribution metrics."

    @staticmethod
    def get_help() -> str:
        return """
Analyze PR review patterns and knowledge distribution metrics.

This command analyzes knowledge sharing patterns within teams by examining PR review data
from LinearB, calculating metrics like review distribution, response times, and knowledge
concentration to identify potential risks and improvement opportunities.

Examples:
  # Analyze knowledge sharing for a specific team over the last week
  python src/main.py linearb knowledge-sharing-metrics --team-ids 19767 --time-range last-week

  # Analyze knowledge sharing for multiple teams over the last month
  python src/main.py linearb knowledge-sharing-metrics --team-ids 19767,41576 --time-range last-month

  # Analyze with custom date range
  python src/main.py linearb knowledge-sharing-metrics --team-ids 19767 --time-range "2025-09-01 to 2025-09-15"

  # Set minimum PR threshold and save results to specific file
  python src/main.py linearb knowledge-sharing-metrics --team-ids 19767 --time-range last-week --pr-threshold 10 --output-file results.json

  # Enable verbose output
  python src/main.py linearb knowledge-sharing-metrics --team-ids 19767 --time-range last-week --verbose

Available team IDs:
  - 19767: Core Services Tribe
  - 41576: Farm Operations Team

Available time ranges:
  - Predefined: last-week, last-2-weeks, last-month
  - Custom: YYYY-MM-DD to YYYY-MM-DD (e.g., "2025-09-01 to 2025-09-15")
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--team-ids",
            type=str,
            required=True,
            help="Comma-separated LinearB team IDs (e.g., '19767,41576')",
        )

        parser.add_argument(
            "--time-range",
            type=str,
            required=True,
            help=(
                "Time period for analysis. "
                "Predefined: last-week, last-2-weeks, last-month. "
                "Custom: YYYY-MM-DD to YYYY-MM-DD (e.g., '2025-09-01 to 2025-09-15')"
            ),
        )

        parser.add_argument(
            "--output-file",
            type=str,
            help="Save results to specific JSON file (optional)",
        )

        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Enable detailed output",
        )

        parser.add_argument(
            "--pr-threshold",
            type=int,
            default=5,
            help="Minimum PRs for inclusion (default: 5)",
        )

    @staticmethod
    def main(args: Namespace):
        # CRITICAL: ALWAYS start with this
        ensure_linearb_env_loaded()

        logger = LogManager.get_instance().get_logger("KnowledgeSharingMetricsCommand")

        try:
            # Parse team IDs
            team_ids = args.team_ids.split(",") if args.team_ids else []

            logger.info(f"Analyzing knowledge sharing metrics for teams: {team_ids}")
            logger.info(f"Time range: {args.time_range}")
            logger.info(f"PR threshold: {args.pr_threshold}")

            # Initialize service
            service = KnowledgeSharingMetricsService()

            # Get knowledge sharing metrics
            results = service.get_knowledge_sharing_metrics(
                team_ids=team_ids,
                time_range=args.time_range,
                pr_threshold=args.pr_threshold,
                verbose=args.verbose,
            )

            # Display summary in the specified format
            metadata = results.get("metadata", {})
            metrics = results.get("metrics", {})
            
            print("=" * 50)
            print("KNOWLEDGE SHARING METRICS ANALYSIS SUMMARY")
            print("=" * 50)
            print(f"Teams: {', '.join(metadata.get('team_ids', []))}")
            print(f"Time Period: {metadata.get('start_date')} to {metadata.get('end_date')}")
            print()
            print(f"Total PRs Reviewed: {metrics.get('total_prs_reviewed', 0)}")
            print(f"Unique Reviewers: {metrics.get('unique_reviewers', 0)}")
            print(f"Average Review Time: {metrics.get('average_review_time_hours', 0)} hours")
            print(f"Bus Factor: {metrics.get('bus_factor', 0)}")
            print(f"Knowledge Distribution Score: {metrics.get('knowledge_distribution_score', 0)}")
            print()
            
            # Display insights
            insights = results.get("insights", [])
            if insights:
                print("Key Insights:")
                for insight in insights:
                    print(f"  - {insight}")
                print()
            
            # Save results if requested
            output_path = None
            if args.output_file:
                output_path = service.save_results(results, args.output_file)
            else:
                # Save to default location
                output_path = service.save_results(results, None)
            
            print(f"Detailed report saved to: {output_path}")
            print("=" * 50)
            
            # Add note about data limitations if needed
            if metrics.get('average_review_time_hours', 0) == 0:
                print("\nNote: Some metrics may be limited due to API permissions or data availability.")
                print("Contact your LinearB administrator for full access to review time data.")

            logger.info("Knowledge sharing metrics analysis completed successfully")

        except ValueError as e:
            print(f"Error: {e}")
            print("Please check your input parameters and try again.")
            exit(1)
        except Exception as e:
            logger.error(f"Knowledge sharing metrics command failed: {e}")
            print(f"Error: {e}")
            print("An unexpected error occurred. Please check the logs for more details.")
            exit(1)