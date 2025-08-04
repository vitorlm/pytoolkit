"""
JIRA Issue Adherence Analysis Command

This command analyzes issue adherence by fetching issues that were resolved within a specified
time period and evaluating their completion status against due dates.

FUNCTIONALITY:
- Fetch issues that were resolved within a time period (last week, last 2 weeks, days)
- Filter issues that have due dates set
- Calculate adherence metrics (on-time, late, overdue)
- Generate detailed adherence reports

USAGE EXAMPLES:

1. Analyze last week's issues:
   python src/main.py syngenta jira issue-adherence --project-key "CWS"
   --time-period "last-week"

2. Analyze last 2 weeks with specific issue types:
   python src/main.py syngenta jira issue-adherence --project-key "CWS"
   --time-period "last-2-weeks" --issue-types "Story,Task"

3. Analyze specific number of days:
   python src/main.py syngenta jira issue-adherence --project-key "CWS"
   --time-period "30-days"

4. Analyze specific date range:
   python src/main.py syngenta jira issue-adherence --project-key "CWS"
   --time-period "2025-06-09 to 2025-06-22"

5. Analyze single specific date:
   python src/main.py syngenta jira issue-adherence --project-key "CWS"
   --time-period "2025-06-15"

6. Analyze with team filter:
   python src/main.py syngenta jira issue-adherence --project-key "CWS"
   --time-period "last-week" --team "Catalog"

7. Export results to file:
   python src/main.py syngenta jira issue-adherence --project-key "CWS"
   --time-period "last-week" --output-file "adherence_report.json"

TIME PERIOD OPTIONS:
- last-week: Issues from the last 7 days
- last-2-weeks: Issues from the last 14 days
- last-month: Issues from the last 30 days
- N-days: Issues from the last N days (e.g., "15-days")
- YYYY-MM-DD to YYYY-MM-DD: Specific date range (e.g., "2025-06-09 to 2025-06-22")
- YYYY-MM-DD: Single specific date (e.g., "2025-06-15")

ADHERENCE METRICS:
- On-time: Issues completed on or before due date
- Late: Issues completed after due date
- Overdue: Issues with due date passed but not completed
- No due date: Issues without due date set
"""

from argparse import ArgumentParser, Namespace

from domains.syngenta.jira.issue_adherence_service import IssueAdherenceService
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager


class IssueAdherenceCommand(BaseCommand):
    """Command to analyze issue adherence against due dates."""

    @staticmethod
    def get_name() -> str:
        return "issue-adherence"

    @staticmethod
    def get_description() -> str:
        return (
            "Analyze issue adherence by checking completion status against due dates."
        )

    @staticmethod
    def get_help() -> str:
        return (
            "This command analyzes issue adherence by fetching issues that were resolved "
            "within a specified time period and evaluating their completion status "
            "against due dates."
        )

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--project-key",
            type=str,
            required=True,
            help="The JIRA project key to analyze (e.g., 'CWS', 'PROJ').",
        )
        parser.add_argument(
            "--time-period",
            type=str,
            required=True,
            help=(
                "Time period to analyze. Options: 'last-week', 'last-2-weeks', "
                "'last-month', 'N-days' (e.g., '30-days'), date ranges "
                "(e.g., '2025-06-09 to 2025-06-22'), or single dates (e.g., '2025-06-15')."
            ),
        )
        parser.add_argument(
            "--issue-types",
            type=str,
            required=False,
            default="Story,Task,Bug",
            help="Comma-separated list of issue types to analyze (default: 'Story,Task,Bug').",
        )
        parser.add_argument(
            "--team",
            type=str,
            required=False,
            help="Filter by team name using Squad[Dropdown] field (optional).",
        )
        parser.add_argument(
            "--status-categories",
            type=str,
            required=False,
            default="Done,In Progress,To Do",
            help=(
                "Comma-separated list of status categories to include "
                "(default: 'Done,In Progress,To Do')."
            ),
        )
        parser.add_argument(
            "--output-file",
            type=str,
            required=False,
            help="Optional file path to save the adherence report in JSON format.",
        )
        parser.add_argument(
            "--include-no-due-date",
            action="store_true",
            help="Include issues without due dates in the analysis.",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Enable verbose output with detailed issue information.",
        )

    @staticmethod
    def main(args: Namespace):
        """
        Main function to execute issue adherence analysis.

        Args:
            args (Namespace): Command-line arguments.
        """
        # Ensure environment variables are loaded
        ensure_env_loaded()

        logger = LogManager.get_instance().get_logger("IssueAdherenceCommand")

        try:
            # Parse issue types
            issue_types = [t.strip() for t in args.issue_types.split(",")]

            # Parse status categories
            status_categories = [s.strip() for s in args.status_categories.split(",")]

            # Initialize service
            service = IssueAdherenceService()

            # Run adherence analysis
            result = service.analyze_issue_adherence(
                project_key=args.project_key,
                time_period=args.time_period,
                issue_types=issue_types,
                team=args.team,
                status_categories=status_categories,
                include_no_due_date=args.include_no_due_date,
                verbose=args.verbose,
                output_file=args.output_file,
            )

            if result:
                logger.info("Issue adherence analysis completed successfully")

                # Print summary
                print("\n" + "=" * 50)
                print("ISSUE ADHERENCE ANALYSIS SUMMARY")
                print("=" * 50)
                print(f"Project: {args.project_key}")
                print(f"Time Period: {args.time_period}")
                print(f"Issue Types: {', '.join(issue_types)}")
                if args.team:
                    print(f"Team: {args.team}")

                # Print metrics
                metrics = result.get("metrics", {})
                print(f"\nTotal Issues Analyzed: {metrics.get('total_issues', 0)}")
                print(
                    f"Issues with Due Dates: {metrics.get('issues_with_due_dates', 0)}"
                )

                early_count = metrics.get("early", 0)
                early_pct = metrics.get("early_percentage", 0)
                print(f"Early Completion: {early_count} ({early_pct:.1f}%)")

                on_time_count = metrics.get("on_time", 0)
                on_time_pct = metrics.get("on_time_percentage", 0)
                print(f"On-time Completion: {on_time_count} ({on_time_pct:.1f}%)")

                late_count = metrics.get("late", 0)
                late_pct = metrics.get("late_percentage", 0)
                print(f"Late Completion: {late_count} ({late_pct:.1f}%)")

                overdue_count = metrics.get("overdue", 0)
                overdue_pct = metrics.get("overdue_percentage", 0)
                print(f"Overdue: {overdue_count} ({overdue_pct:.1f}%)")

                if args.include_no_due_date:
                    no_due_count = metrics.get("no_due_date", 0)
                    no_due_pct = metrics.get("no_due_date_percentage", 0)
                    print(f"No Due Date: {no_due_count} ({no_due_pct:.1f}%)")

                adherence_rate = metrics.get("adherence_rate", 0)
                print(f"\nOverall Adherence Rate: {adherence_rate:.1f}%")

                if args.output_file:
                    print(f"\nDetailed report saved to: {args.output_file}")

                print("=" * 50)
            else:
                logger.error("Issue adherence analysis failed")
                exit(1)

        except Exception as e:
            # Check if this is a JQL/issue type error and provide helpful guidance
            error_str = str(e)
            if ("400" in error_str and ("does not exist for the field 'type'" in error_str)):
                logger.error(f"Failed to execute issue adherence analysis: {e}")
                print(f"\n{'='*50}")
                print("ERROR: Invalid Issue Type Detected")
                print("="*50)
                print(f"Error: {e}")
                print(f"\nThe command failed because one or more issue types are not valid for project {args.project_key}.")
                print(f"You provided: {args.issue_types}")
                print(f"\nTo fix this issue:")
                print(f"1. Use the list-custom-fields command to see available issue types:")
                print(f"   python src/main.py syngenta jira list-custom-fields --project-key {args.project_key}")
                print(f"2. Or try with common issue types:")
                print(f"   --issue-types 'Bug,Story,Task,Epic'")
                print("="*50)
            else:
                logger.error(f"Failed to execute issue adherence analysis: {e}")
                print(f"Error: Failed to execute issue adherence analysis: {e}")
            exit(1)
