"""
JIRA Issue Velocity Analysis Command

This command analyzes issue velocity by tracking both creation and resolution rates monthly,
providing insights into team productivity and backlog trends.

FUNCTIONALITY:
- Fetch issues created within a specified time period
- Fetch issues resolved within the same time period
- Calculate monthly velocity metrics (created vs resolved)
- Analyze team productivity trends and backlog impact
- Support flexible filtering by team, issue types, and labels

USAGE EXAMPLES:

1. Basic velocity analysis for last 6 months (bugs only):
   python src/main.py syngenta jira issue-velocity --project-key "CWS"
   --time-period "last-6-months"

2. Team-specific analysis:
   python src/main.py syngenta jira issue-velocity --project-key "CWS"
   --time-period "last-year" --team "Catalog"

3. Multiple issue types analysis:
   python src/main.py syngenta jira issue-velocity --project-key "CWS"
   --time-period "last-6-months" --issue-types "Bug,Story,Task"

4. Filter by labels:
   python src/main.py syngenta jira issue-velocity --project-key "CWS"
   --time-period "last-6-months" --labels "priority,urgent"

5. Export to CSV with summary:
   python src/main.py syngenta jira issue-velocity --project-key "CWS"
   --time-period "last-year" --export "csv" --include-summary

6. Quarterly aggregation:
   python src/main.py syngenta jira issue-velocity --project-key "CWS"
   --time-period "last-year" --aggregation "quarterly"

TIME PERIOD OPTIONS:
- last-6-months: Issues from the last 6 months
- last-year: Issues from the last 12 months
- last-2-years: Issues from the last 24 months
- YYYY-MM-DD to YYYY-MM-DD: Specific date range (e.g., "2024-01-01 to 2024-12-31")

VELOCITY METRICS:
- Monthly/Quarterly created vs resolved counts
- Net velocity (resolved - created) per period
- Efficiency percentage (resolved/created * 100)
- Backlog impact (cumulative net velocity)
- Velocity trends and performance analysis
- Breakdown by issue type and team
"""

from argparse import ArgumentParser, Namespace

from domains.syngenta.jira.issue_velocity_service import IssueVelocityService
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager


class IssueVelocityCommand(BaseCommand):
    """Command to analyze issue velocity through creation vs resolution rates."""

    @staticmethod
    def get_name() -> str:
        return "issue-velocity"

    @staticmethod
    def get_description() -> str:
        return "Analyze monthly issue creation vs resolution velocity with team and backlog trends."

    @staticmethod
    def get_help() -> str:
        return (
            "This command analyzes issue velocity by tracking both creation and resolution "
            "rates monthly, calculating net velocity, efficiency, and backlog trends. "
            "Supports filtering by team, issue types, and labels."
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
                "Time period to analyze. Options: 'last-6-months', 'last-year', "
                "'last-2-years', or date ranges (e.g., '2024-01-01 to 2024-12-31')."
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
            "--labels",
            type=str,
            required=False,
            help="Comma-separated list of labels to filter (optional).",
        )
        parser.add_argument(
            "--aggregation",
            type=str,
            required=False,
            default="monthly",
            choices=["monthly", "quarterly"],
            help="Aggregation granularity for results (default: monthly).",
        )
        parser.add_argument(
            "--export",
            type=str,
            required=False,
            choices=["json", "csv"],
            help="Export format for results (json or csv).",
        )
        parser.add_argument(
            "--output-file",
            type=str,
            required=False,
            help="Optional file path to save the velocity report.",
        )
        parser.add_argument(
            "--include-summary",
            action="store_true",
            help="Include detailed summary statistics in output.",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Enable verbose output with detailed issue information.",
        )
        parser.add_argument(
            "--clear-cache",
            action="store_true",
            help="Clear cache before running analysis.",
        )

    @staticmethod
    def main(args: Namespace):
        """
        Main function to execute issue velocity analysis.

        Args:
            args (Namespace): Command-line arguments.
        """
        # Ensure environment variables are loaded
        ensure_env_loaded()

        logger = LogManager.get_instance().get_logger("IssueVelocityCommand")

        try:
            # Parse issue types
            issue_types = [t.strip() for t in args.issue_types.split(",")]

            # Parse labels
            labels = None
            if args.labels:
                labels = [label.strip() for label in args.labels.split(",")]

            # Initialize service
            service = IssueVelocityService()

            # Clear cache if requested
            if args.clear_cache:
                service.clear_cache()
                logger.info("Cache cleared successfully")

            # Run velocity analysis
            result = service.analyze_issue_velocity(
                project_key=args.project_key,
                time_period=args.time_period,
                issue_types=issue_types,
                team=args.team,
                labels=labels,
                aggregation=args.aggregation,
                include_summary=args.include_summary,
                verbose=args.verbose,
                output_file=args.output_file,
                export=args.export,
            )

            if result:
                logger.info("Issue velocity analysis completed successfully")

                # Print summary
                IssueVelocityCommand._print_velocity_summary(result, args, issue_types)

                if args.output_file or args.export:
                    output_file = result.get("output_file")
                    if output_file:
                        print(f"\n💾 Results exported to: {output_file}")

            else:
                logger.error("Issue velocity analysis failed")
                exit(1)

        except Exception as e:
            # Check for common errors and provide helpful guidance
            error_str = str(e)
            if "400" in error_str and ("does not exist for the field 'type'" in error_str):
                logger.error(f"Failed to execute velocity analysis: {e}")
                print("\n" + "=" * 70)
                print("ERROR: Invalid Issue Type Detected")
                print("=" * 70)
                print(f"Error: {e}")
                print(
                    f"\nThe command failed because one or more issue types are not valid for project {args.project_key}."
                )
                print(f"You provided: {args.issue_types}")
                print("\nTo fix this issue:")
                print("1. Use the list-custom-fields command to see available issue types:")
                print(
                    f"   python src/main.py syngenta jira list-custom-fields --project-key {args.project_key}"
                )
                print("2. Or try with common issue types:")
                print("   --issue-types 'Bug,Story,Task,Epic'")
                print("=" * 70)
            elif "Invalid time period format" in error_str:
                logger.error(f"Failed to execute velocity analysis: {e}")
                print("\n" + "=" * 70)
                print("ERROR: Invalid Time Period Format")
                print("=" * 70)
                print(f"Error: {e}")
                print("\nValid time period formats:")
                print("• last-6-months, last-year, last-2-years")
                print("• Date ranges: 2024-01-01 to 2024-12-31")
                print("=" * 70)
            else:
                logger.error(f"Failed to execute velocity analysis: {e}")
                print(f"Error: Failed to execute velocity analysis: {e}")
            exit(1)

    @staticmethod
    def _print_velocity_summary(result: dict, args: Namespace, issue_types: list):
        """
        Print the summary results of the velocity analysis.

        Args:
            result (dict): Analysis results
            args (Namespace): Command arguments
            issue_types (list): List of issue types analyzed
        """
        if not result or "velocity_data" not in result:
            return

        print("\n" + "=" * 80)
        print("ISSUE VELOCITY ANALYSIS")
        print("=" * 80)
        print(f"Project: {args.project_key}")
        print(f"Period: {result.get('period_display', args.time_period)}")
        print(f"Issue Types: {', '.join(issue_types)}")
        if args.team:
            print(f"Team: {args.team}")
        if args.labels:
            print(f"Labels: {', '.join(args.labels)}")
        print(f"Aggregation: {args.aggregation.title()}")
        print()

        velocity_data = result["velocity_data"]
        if not velocity_data:
            print("No data found for the specified criteria.")
            return

        # Print velocity table
        print(f"{args.aggregation.upper()} VELOCITY SUMMARY")
        print("-" * 80)

        # Header
        period_label = "Month" if args.aggregation == "monthly" else "Quarter"
        print(
            f"{'Period':<12} {'Created':<8} {'Resolved':<9} {'Net Vel':<8} "
            f"{'Backlog':<8} {'Efficiency':<10}"
        )
        print(f"{'=' * 12} {'=' * 8} {'=' * 9} {'=' * 8} " f"{'=' * 8} {'=' * 10}")

        # Data rows
        cumulative_backlog = 0
        for period_data in velocity_data:
            period = period_data["period"]
            created = period_data["created"]
            resolved = period_data["resolved"]
            net_velocity = period_data["net_velocity"]
            efficiency = period_data["efficiency_percentage"]

            cumulative_backlog += net_velocity
            backlog_display = (
                f"+{cumulative_backlog}" if cumulative_backlog > 0 else str(cumulative_backlog)
            )

            print(
                f"{period:<12} {created:<8} {resolved:<9} {net_velocity:>+7} "
                f"{backlog_display:<8} {efficiency:>8.1f}%"
            )

        # Print totals
        summary = result.get("summary_statistics", {})
        if summary:
            print("-" * 80)
            total_created = summary.get("total_created", 0)
            total_resolved = summary.get("total_resolved", 0)
            net_total = summary.get("net_velocity", 0)
            overall_efficiency = summary.get("overall_efficiency", 0)

            print(
                f"{'TOTALS:':<12} {total_created:<8} {total_resolved:<9} {net_total:>+7} "
                f"{'+' + str(cumulative_backlog) if cumulative_backlog > 0 else str(cumulative_backlog):<8} {overall_efficiency:>8.1f}%"
            )

        # Print additional statistics if available
        if args.include_summary and summary:
            print("\nVELOCITY TRENDS")
            print("-" * 50)
            avg_created = summary.get("average_created", 0)
            avg_resolved = summary.get("average_resolved", 0)
            print(f"• Average {period_label} Created: {avg_created:.1f} issues")
            print(f"• Average {period_label} Resolved: {avg_resolved:.1f} issues")

            best_period = summary.get("best_period")
            worst_period = summary.get("worst_period")
            if best_period:
                print(
                    f"• Best {period_label} (Efficiency): {best_period['period']} ({best_period['efficiency']:.1f}%)"
                )
            if worst_period:
                print(
                    f"• Worst {period_label} (Efficiency): {worst_period['period']} ({worst_period['efficiency']:.1f}%)"
                )

            trend = summary.get("trend_direction", "stable")
            print(f"• Trend Direction: {trend.title()}")

            backlog_change = summary.get("backlog_change", 0)
            print(
                f"• Total Backlog Change: {'+' if backlog_change > 0 else ''}{backlog_change} issues"
            )

        # Print breakdown by issue type if multiple types
        if len(issue_types) > 1 and "by_issue_type" in result:
            print("\nBREAKDOWN BY ISSUE TYPE")
            print("-" * 50)
            for issue_type, type_data in result["by_issue_type"].items():
                created = type_data.get("total_created", 0)
                resolved = type_data.get("total_resolved", 0)
                net = type_data.get("net_velocity", 0)
                print(
                    f"{issue_type:<15} Created: {created:<4} | Resolved: {resolved:<4} | Net: {net:>+3}"
                )

        print("=" * 80)
