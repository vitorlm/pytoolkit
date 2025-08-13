"""
JIRA Cycle Time Analysis Command

This command analyzes cycle time by calculating the time taken for tickets to move from
Started status (07) to Done status (10), excluding Archived tickets (11).

FUNCTIONALITY:
- Fetch issues that were resolved within a specified time period
- Filter issues that have transitioned from Started to Done
- Calculate cycle time metrics (time between Started and Done statuses)
- Generate detailed cycle time reports
- Exclude Archived tickets from analysis

USAGE EXAMPLES:

1. Analyze last week's issues:
   python src/main.py syngenta jira cycle-time --project-key "CWS"
   --time-period "last-week"

2. Analyze last 2 weeks with specific issue types:
   python src/main.py syngenta jira cycle-time --project-key "CWS"
   --time-period "last-2-weeks" --issue-types "Story,Task"

3. Analyze specific number of days:
   python src/main.py syngenta jira cycle-time --project-key "CWS"
   --time-period "30-days"

4. Analyze specific date range:
   python src/main.py syngenta jira cycle-time --project-key "CWS"
   --time-period "2025-06-09 to 2025-06-22"

5. Analyze single specific date:
   python src/main.py syngenta jira cycle-time --project-key "CWS"
   --time-period "2025-06-15"

6. Analyze with team filter:
   python src/main.py syngenta jira cycle-time --project-key "CWS"
   --time-period "last-week" --team "Catalog"

7. Export results to file:
   python src/main.py syngenta jira cycle-time --project-key "CWS"
   --time-period "last-week" --output-file "cycle_time_report.json"

TIME PERIOD OPTIONS:
- last-week: Issues from the last 7 days
- last-2-weeks: Issues from the last 14 days
- last-month: Issues from the last 30 days
- N-days: Issues from the last N days (e.g., "15-days")
- YYYY-MM-DD to YYYY-MM-DD: Specific date range (e.g., "2025-06-09 to 2025-06-22")
- YYYY-MM-DD: Single specific date (e.g., "2025-06-15")

CYCLE TIME METRICS:
- Average cycle time: Average time from Started to Done
- Median cycle time: Median time from Started to Done
- Min/Max cycle times: Fastest and slowest resolution times
- Distribution by time ranges (e.g., < 1 day, 1-3 days, etc.)
"""

from argparse import ArgumentParser, Namespace

from domains.syngenta.jira.cycle_time_service import CycleTimeService
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager


class CycleTimeCommand(BaseCommand):
    """Command to analyze cycle time from Started to Done status."""

    @staticmethod
    def get_name() -> str:
        return "cycle-time"

    @staticmethod
    def get_description() -> str:
        return "Analyze cycle time by calculating time from Started to Done status."

    @staticmethod
    def get_help() -> str:
        return (
            "This command analyzes cycle time by calculating the time taken for tickets "
            "to move from Started status (07) to Done status (10), excluding Archived "
            "tickets (11)."
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
            default="Bug",
            help="Comma-separated list of issue types to analyze (default: 'Bug').",
        )
        parser.add_argument(
            "--team",
            type=str,
            required=False,
            help="Filter by team name using Squad[Dropdown] field (optional).",
        )
        parser.add_argument(
            "--priority",
            type=str,
            required=False,
            help="Filter by priority (e.g., 'Critical', 'High', 'Medium', 'Low'). Comma-separated for multiple priorities.",
        )
        parser.add_argument(
            "--status-categories",
            type=str,
            required=False,
            default="Done",
            help=(
                "Comma-separated list of status categories to include "
                "(default: 'Done')."
            ),
        )
        parser.add_argument(
            "--output-file",
            type=str,
            required=False,
            help="Optional file path to save the cycle time report in JSON format.",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Enable verbose output with detailed issue information.",
        )

    @staticmethod
    def main(args: Namespace):
        """
        Main function to execute cycle time analysis.

        Args:
            args (Namespace): Command-line arguments.
        """
        # Ensure environment variables are loaded
        ensure_env_loaded()

        logger = LogManager.get_instance().get_logger("CycleTimeCommand")

        try:
            # Parse issue types
            issue_types = [t.strip() for t in args.issue_types.split(",")]

            # Parse status categories
            status_categories = [s.strip() for s in args.status_categories.split(",")]

            # Parse priorities
            priorities = None
            if args.priority:
                priorities = [p.strip() for p in args.priority.split(",")]

            # Initialize service
            service = CycleTimeService()

            # Run cycle time analysis
            result = service.analyze_cycle_time(
                project_key=args.project_key,
                time_period=args.time_period,
                issue_types=issue_types,
                team=args.team,
                priorities=priorities,
                status_categories=status_categories,
                verbose=args.verbose,
                output_file=args.output_file,
            )

            if result:
                logger.info("Cycle time analysis completed successfully")

                # Print summary
                print("\n" + "=" * 50)
                print("CYCLE TIME ANALYSIS SUMMARY")
                print("=" * 50)
                print(f"Project: {args.project_key}")
                print(f"Time Period: {args.time_period}")
                print(f"Issue Types: {', '.join(issue_types)}")
                if args.team:
                    print(f"Team: {args.team}")
                if priorities:
                    print(f"Priority Filter: {', '.join(priorities)}")

                # Print metrics
                metrics = result.get("metrics", {})
                print(f"\nTotal Issues Analyzed: {metrics.get('total_issues', 0)}")
                print(
                    f"Issues with Cycle Time: {metrics.get('issues_with_cycle_time', 0)}"
                )

                avg_cycle_time = metrics.get("average_cycle_time_hours", 0)
                avg_days = avg_cycle_time / 24
                print(
                    f"Average Cycle Time: {avg_cycle_time:.1f} hours ({avg_days:.1f} days)"
                )

                median_cycle_time = metrics.get("median_cycle_time_hours", 0)
                median_days = median_cycle_time / 24
                print(
                    f"Median Cycle Time: {median_cycle_time:.1f} hours ({median_days:.1f} days)"
                )

                min_cycle_time = metrics.get("min_cycle_time_hours", 0)
                min_days = min_cycle_time / 24
                print(
                    f"Fastest Resolution: {min_cycle_time:.1f} hours ({min_days:.1f} days)"
                )

                max_cycle_time = metrics.get("max_cycle_time_hours", 0)
                max_days = max_cycle_time / 24
                print(
                    f"Slowest Resolution: {max_cycle_time:.1f} hours ({max_days:.1f} days)"
                )

                # Print distribution
                distribution = metrics.get("time_distribution", {})
                if distribution:
                    print("\nTime Distribution:")
                    for time_range, count in distribution.items():
                        total_issues = metrics.get("issues_with_cycle_time", 1)
                        percentage = (count / total_issues) * 100
                        print(f"  {time_range}: {count} issues ({percentage:.1f}%)")

                # Print priority breakdown
                priority_metrics = metrics.get("priority_breakdown", {})
                if priority_metrics:
                    print("\nCycle Time by Priority:")
                    for priority, p_metrics in priority_metrics.items():
                        avg_hours = p_metrics.get("average_cycle_time_hours", 0)
                        avg_days = avg_hours / 24
                        count = p_metrics.get("count", 0)
                        print(
                            f"  {priority}: {count} issues, avg {avg_hours:.1f}h ({avg_days:.1f}d)"
                        )

                if args.output_file:
                    print(f"\nDetailed report saved to: {args.output_file}")

                print("=" * 50)
            else:
                logger.error("Cycle time analysis failed")
                exit(1)

        except Exception as e:
            # Check if this is a JQL/issue type error and provide helpful guidance
            error_str = str(e)
            if "400" in error_str and (
                "does not exist for the field 'type'" in error_str
            ):
                logger.error(f"Failed to execute cycle time analysis: {e}")
                print(f"\n{'='*50}")
                print("ERROR: Invalid Issue Type Detected")
                print("=" * 50)
                print(f"Error: {e}")
                print(
                    f"\nThe command failed because one or more issue types are not valid for project {args.project_key}."
                )
                print(f"You provided: {args.issue_types}")
                print("\nTo fix this issue:")
                print(
                    "1. Use the list-custom-fields command to see available issue types:"
                )
                print(
                    f"   python src/main.py syngenta jira list-custom-fields --project-key {args.project_key}"
                )
                print("2. Or try with common issue types:")
                print("   --issue-types 'Bug,Story,Task,Epic'")
                print("=" * 50)
            else:
                logger.error(f"Failed to execute cycle time analysis: {e}")
                print(f"Error: Failed to execute cycle time analysis: {e}")
            exit(1)
