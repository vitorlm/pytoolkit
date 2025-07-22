from argparse import ArgumentParser, Namespace
from datetime import datetime

from domains.syngenta.jira.issues_creation_analysis_service import (
    IssuesCreationAnalysisService,
)
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager


class IssuesCreationAnalysisCommand(BaseCommand):
    @staticmethod
    def get_name() -> str:
        return "issues-creation-analysis"

    @staticmethod
    def get_description() -> str:
        return "Analyze JIRA issues creation patterns over time with configurable aggregation"

    @staticmethod
    def get_help() -> str:
        return """
        Analyze the number of JIRA issues created over time with flexible filtering options.

        Features:
        - Time range filtering (start/end dates)
        - Aggregation by day, week, or month
        - Issue type filtering (Bug, Task, Story, etc.)
        - Project and label filtering
        - Export results to CSV or JSON

        Examples:
        # Basic usage - last 30 days, daily aggregation
        python src/main.py syngenta jira issues-creation-analysis

        # Custom date range with weekly aggregation
        python src/main.py syngenta jira issues-creation-analysis \
            --start-date 2024-01-01 --end-date 2024-03-31 --aggregation weekly

        # Filter by issue types and export to CSV
        python src/main.py syngenta jira issues-creation-analysis \
            --issue-types Bug,Task --export csv --output-file issues_analysis.csv

        # Advanced filtering with JQL
        python src/main.py syngenta jira issues-creation-analysis \
            --jql "project = 'Cropwise Core Services' AND labels = 'priority'"
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--start-date",
            type=str,
            help="Start date for analysis (YYYY-MM-DD format). Default: 30 days ago",
        )
        parser.add_argument(
            "--end-date",
            type=str,
            help="End date for analysis (YYYY-MM-DD format). Default: today",
        )
        parser.add_argument(
            "--time-period",
            type=str,
            help="Time period for analysis (e.g., 'last-week', 'last-month', 'last-2-years'). "
            "Alternative to start-date/end-date",
        )
        parser.add_argument(
            "--aggregation",
            choices=["daily", "weekly", "monthly"],
            default="daily",
            help="Aggregation granularity for results (default: daily)",
        )
        parser.add_argument(
            "--issue-types",
            type=str,
            help="Comma-separated list of issue types to filter (e.g., Bug,Task,Story)",
        )
        parser.add_argument(
            "--projects",
            type=str,
            help="Comma-separated list of project keys to filter (e.g., CCS,PROJ)",
        )
        parser.add_argument(
            "--labels",
            type=str,
            help="Comma-separated list of labels to filter",
        )
        parser.add_argument(
            "--jql",
            type=str,
            help="Additional JQL filter to apply (will be combined with other filters)",
        )
        parser.add_argument(
            "--export",
            choices=["json", "csv"],
            help="Export format for results",
        )
        parser.add_argument(
            "--output-file",
            type=str,
            help="Output file path for export (default: auto-generated based on format)",
        )
        parser.add_argument(
            "--include-summary",
            action="store_true",
            help="Include summary statistics in output",
        )
        parser.add_argument(
            "--clear-cache",
            action="store_true",
            help="Clear cache before running analysis",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Enable verbose output with date-by-date logging",
        )

    @staticmethod
    def main(args: Namespace):
        ensure_env_loaded()

        logger = LogManager.get_instance().get_logger("IssuesCreationAnalysisCommand")

        try:
            logger.info("Starting JIRA issues creation analysis")

            service = IssuesCreationAnalysisService()

            if args.clear_cache:
                service.clear_cache()
                logger.info("Cache cleared successfully")

            # Parse issue types
            issue_types = None
            if args.issue_types:
                issue_types = [t.strip() for t in args.issue_types.split(",")]

            # Parse projects
            projects = None
            if args.projects:
                projects = [p.strip() for p in args.projects.split(",")]

            # Parse labels
            labels = None
            if args.labels:
                labels = [label.strip() for label in args.labels.split(",")]

            # Parse dates
            start_date = None
            end_date = None

            if args.start_date:
                try:
                    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
                except ValueError:
                    logger.error(
                        f"Invalid start date format: {args.start_date}. Use YYYY-MM-DD"
                    )
                    exit(1)

            if args.end_date:
                try:
                    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
                except ValueError:
                    logger.error(
                        f"Invalid end date format: {args.end_date}. Use YYYY-MM-DD"
                    )
                    exit(1)

            # Run analysis
            result = service.analyze_issues_creation(
                start_date=start_date,
                end_date=end_date,
                aggregation=args.aggregation,
                issue_types=issue_types,
                projects=projects,
                labels=labels,
                additional_jql=args.jql,
                include_summary=args.include_summary,
                time_period=args.time_period,
                verbose=args.verbose,
            )

            logger.info(
                f"Analysis completed. Found {len(result.get('data', []))} data points"
            )

            # Export results if requested
            output_file = None
            if args.export:
                output_file = service.export_results(
                    result,
                    format=args.export,
                    output_file=args.output_file,
                )
                logger.info(f"Results exported to: {output_file}")
            else:
                # Display results in console
                service.display_results(result, verbose=args.verbose)

            # Log the output file path - service always creates output files
            if not output_file and "output_file" in result:
                output_file = result["output_file"]
                logger.info(f"Results automatically saved to: {output_file}")

            logger.info("JIRA issues creation analysis completed successfully")

        except Exception as e:
            logger.error(f"Issues creation analysis failed: {e}", exc_info=True)
            exit(1)
