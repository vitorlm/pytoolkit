from argparse import ArgumentParser, Namespace
from datetime import date, datetime, timedelta

from domains.syngenta.jira.issues_creation_analysis_service import (
    IssuesCreationAnalysisService,
)
from domains.syngenta.jira.shared.parsers import ErrorHandler
from domains.syngenta.jira.summary.jira_summary_manager import JiraSummaryManager
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
        - Time range filtering with anchor date (end-date) and window (window-days)
        - Aggregation by day, week, or month
        - Issue type filtering (Bug, Task, Story, etc.)
        - Team filtering using Squad[Dropdown] field
        - Label filtering
        - Export results to CSV, JSON, or Markdown

        Examples:
        # Basic usage - last 30 days, daily aggregation
        python src/main.py syngenta jira issues-creation-analysis --project-key "CWS"

        # Custom window (90 days)
        python src/main.py syngenta jira issues-creation-analysis --project-key "CWS" \
            --window-days 90

        # Specific anchor date with window
        python src/main.py syngenta jira issues-creation-analysis --project-key "CWS" \
            --end-date 2024-03-31 --window-days 90 --aggregation weekly

        # Filter by team and issue types
        python src/main.py syngenta jira issues-creation-analysis --project-key "CWS" \
            --window-days 180 --teams "Catalog,Platform" --issue-types "Bug,Story"

        # Export to Markdown with summary
        python src/main.py syngenta jira issues-creation-analysis --project-key "CWS" \
            --window-days 30 --output-format md --include-summary
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--project-key",
            type=str,
            required=True,
            help="The JIRA project key to analyze (e.g., 'CWS', 'PROJ').",
        )
        # Time window (anchor + window)
        parser.add_argument(
            "--end-date",
            type=str,
            required=False,
            help=(
                "Anchor date in YYYY-MM-DD. If provided with --window-days, defines the analysis window. Defaults to today."
            ),
        )
        parser.add_argument(
            "--window-days",
            type=int,
            required=False,
            default=30,
            help="Window size in days counting backwards from end-date (default: 30).",
        )
        parser.add_argument(
            "--aggregation",
            type=str,
            required=False,
            choices=["daily", "weekly", "monthly"],
            default="daily",
            help="Aggregation granularity for results (default: daily)",
        )
        parser.add_argument(
            "--issue-types",
            type=str,
            required=False,
            default="Story,Task,Bug,Epic",
            help="Comma-separated list of issue types to filter (default: 'Story,Task,Bug,Epic')",
        )
        parser.add_argument(
            "--team",
            "--teams",
            dest="teams",
            action="append",
            required=False,
            help=(
                "Filter by one or more teams using Squad[Dropdown] field. "
                "You can repeat --team/--teams or pass a comma-separated list (e.g., 'Catalog,Platform')."
            ),
        )
        parser.add_argument(
            "--labels",
            type=str,
            required=False,
            help="Comma-separated list of labels to filter",
        )
        parser.add_argument(
            "--jql",
            type=str,
            required=False,
            help="Additional JQL filter to apply (will be combined with other filters)",
        )
        parser.add_argument(
            "--output-format",
            type=str,
            required=False,
            choices=["json", "md", "console"],
            default="console",
            help="Output format: json (JSON file), md (Markdown file), console (display only)",
        )
        parser.add_argument(
            "--summary-output",
            type=str,
            choices=["auto", "json", "none"],
            default="auto",
            help=(
                "Control summary metrics persistence: 'auto' stores alongside generated reports, "
                "'json' forces a summary file even without a report, and 'none' skips it."
            ),
        )
        parser.add_argument(
            "--output-file",
            type=str,
            required=False,
            help="Optional file path to save the analysis report.",
        )
        parser.add_argument(
            "--include-summary",
            action="store_true",
            help="Include detailed summary statistics in output",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Enable verbose output with date-by-date logging",
        )
        parser.add_argument(
            "--clear-cache",
            action="store_true",
            help="Clear cache before running analysis",
        )

    @staticmethod
    def main(args: Namespace):
        ensure_env_loaded()

        logger = LogManager.get_instance().get_logger("IssuesCreationAnalysisCommand")

        try:
            logger.info("Starting JIRA issues creation analysis")

            # Parse issue types
            issue_types = [t.strip() for t in args.issue_types.split(",")]

            # Parse teams (supports repeated flags and comma-separated lists)
            raw_teams = getattr(args, "teams", []) or []
            teams = [t.strip() for entry in raw_teams for t in entry.split(",") if t.strip()] or None

            # Parse labels
            labels = None
            if args.labels:
                labels = [label.strip() for label in args.labels.split(",")]

            # Parse dates using end_date + window_days pattern
            end_date = date.today()
            if args.end_date:
                try:
                    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
                except ValueError:
                    logger.error(f"Invalid end date format: {args.end_date}. Use YYYY-MM-DD")
                    raise

            window_days = args.window_days
            start_date = end_date - timedelta(days=window_days)

            # Initialize service
            service = IssuesCreationAnalysisService()

            if args.clear_cache:
                service.clear_cache()
                logger.info("Cache cleared successfully")

            # Run analysis
            result = service.analyze_issues_creation(
                project_key=args.project_key,
                start_date=start_date,
                end_date=end_date,
                aggregation=args.aggregation,
                issue_types=issue_types,
                teams=teams,
                labels=labels,
                additional_jql=args.jql,
                include_summary=args.include_summary,
                verbose=args.verbose,
                output_file=args.output_file,
                output_format=args.output_format,
            )

            if result:
                logger.info("Issue creation analysis completed successfully")

                # Display results based on output format
                if args.output_format == "console":
                    service.display_results(result, verbose=args.verbose)
                else:
                    output_file = result.get("output_file")
                    if output_file:
                        print(f"\n💾 Results exported to: {output_file}")

                # Generate summary metrics using JiraSummaryManager
                try:
                    summary_mode = getattr(args, "summary_output", "auto")
                    raw_output_path = result.get("output_file")

                    # Use JiraSummaryManager for summary generation
                    summary_manager = JiraSummaryManager()
                    args.command_name = "issues-creation-analysis"  # Set command name for metrics
                    summary_path = summary_manager.emit_summary_compatible(
                        result,
                        summary_mode,
                        raw_output_path,
                        args,
                    )
                    if summary_path:
                        print(f"[summary] wrote: {summary_path}")
                except Exception as summary_error:
                    logger.warning(f"Failed to write summary metrics: {summary_error}")

            else:
                logger.error("Issue creation analysis failed")
                exit(1)

        except Exception as e:
            logger.error(f"Failed to execute issue creation analysis: {e}")

            # Use ErrorHandler for consistent error messaging
            error_handler = ErrorHandler()
            error_handler.handle_api_error(e, f"issues creation analysis for project {args.project_key}")
            exit(1)
