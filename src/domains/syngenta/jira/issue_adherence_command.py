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
        return "Analyze issue adherence by checking completion status against due dates."

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
            "--status",
            type=str,
            required=False,
            default="10 Done",
            help=("Comma-separated list of status to include (default: '10 Done')."),
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
        parser.add_argument(
            "--output-format",
            type=str,
            choices=["json", "md", "console"],
            default="console",
            help="Output format: json (JSON file), md (Markdown file), console (display only)",
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

            # Parse status
            status = [s.strip() for s in args.status.split(",")]

            # Initialize service
            service = IssueAdherenceService()

            # Run adherence analysis
            result = service.analyze_issue_adherence(
                project_key=args.project_key,
                time_period=args.time_period,
                issue_types=issue_types,
                team=args.team,
                status=status,
                include_no_due_date=args.include_no_due_date,
                verbose=args.verbose,
                output_file=args.output_file,
            )

            if result:
                logger.info("Issue adherence analysis completed successfully")

                # Handle different output formats
                if args.output_format == "console":
                    # For now, use the basic summary (will be enhanced)
                    IssueAdherenceCommand._print_basic_summary(result, args)
                elif args.output_format in ["json", "md"]:
                    output_path = result.get("output_file")
                    if output_path:
                        print("✅ Issue adherence analysis completed successfully!")
                        print(f"📄 Report saved to: {output_path}")
                    else:
                        print("⚠️  Analysis completed but no output file was generated.")

            else:
                logger.error("Issue adherence analysis failed")
                exit(1)

        except Exception as e:
            # Check if this is a JQL/issue type error and provide helpful guidance
            error_str = str(e)
            if "400" in error_str and ("does not exist for the field 'type'" in error_str):
                logger.error(f"Failed to execute issue adherence analysis: {e}")
                print(f"\n{'=' * 50}")
                print("ERROR: Invalid Issue Type Detected")
                print("=" * 50)
                print(f"Error: {e}")
                print(
                    f"\nThe command failed because one or more issue types are not valid for project {args.project_key}."
                )
                print(f"You provided: {args.issue_types}")
                print("\nTo fix this issue:")
                print("1. Use the list-custom-fields command to see available issue types:")
                print(f"   python src/main.py syngenta jira list-custom-fields --project-key {args.project_key}")
                print("2. Or try with common issue types:")
                print("   --issue-types 'Bug,Story,Task,Epic'")
                print("=" * 50)
            else:
                logger.error(f"Failed to execute issue adherence analysis: {e}")
                print(f"Error: Failed to execute issue adherence analysis: {e}")
            exit(1)

    @staticmethod
    def _print_basic_summary(result: dict, args: Namespace):
        """Print enhanced adherence report to console with rich formatting."""
        issue_types = [t.strip() for t in args.issue_types.split(",")]
        metrics = result.get("metrics", {})

        # Helper functions for formatting
        def get_risk_emoji(adherence_rate: float) -> str:
            if adherence_rate >= 80:
                return "🟢"  # Low risk
            elif adherence_rate >= 60:
                return "🟡"  # Medium risk
            else:
                return "🔴"  # High risk

        def get_risk_assessment(adherence_rate: float) -> str:
            if adherence_rate >= 80:
                return "Low Risk - Excellent adherence"
            elif adherence_rate >= 60:
                return "Medium Risk - Some concerns"
            else:
                return "High Risk - Action required"

        # Extract key metrics
        adherence_rate = metrics.get("adherence_rate", 0)
        total_issues = metrics.get("total_issues", 0)
        issues_with_due_dates = metrics.get("issues_with_due_dates", 0)

        risk_emoji = get_risk_emoji(adherence_rate)
        risk_assessment = get_risk_assessment(adherence_rate)

        # Header
        print("\n" + "🎯" + "=" * 77)
        print(f"   {risk_emoji} JIRA ISSUE ADHERENCE ANALYSIS REPORT")
        print("=" * 79)

        # Project Information
        print(f"📊 PROJECT: {args.project_key} | 📅 PERIOD: {args.time_period}")
        if args.team:
            print(f"👥 TEAM: {args.team}")
        print(f"🏷️  ISSUE TYPES: {', '.join(issue_types)}")
        print("-" * 79)

        # Executive Summary
        print("📈 EXECUTIVE SUMMARY")
        print(f"   Overall Adherence Rate: {adherence_rate:.1f}%")
        print(f"   Risk Assessment: {risk_assessment}")
        print(f"   Total Issues Analyzed: {total_issues}")
        print(f"   Issues with Due Dates: {issues_with_due_dates}")
        print()

        # Adherence Breakdown
        print("📋 ADHERENCE BREAKDOWN")

        status_data = [
            ("early", "Early Completion", "🟢"),
            ("on_time", "On-time Completion", "✅"),
            ("late", "Late Completion", "🟡"),
            ("overdue", "Overdue", "🔴"),
            ("in_progress", "In Progress", "🔵"),
        ]

        if args.include_no_due_date:
            status_data.append(("no_due_date", "No Due Date", "⚪"))

        # Print status breakdown in a formatted way
        for status_key, status_label, emoji in status_data:
            count = metrics.get(status_key, 0)
            percentage = metrics.get(f"{status_key}_percentage", 0)

            if count > 0 or status_key in ["early", "on_time", "late", "overdue"]:
                print(f"   {emoji} {status_label:<20} {count:>3} issues ({percentage:>5.1f}%)")

        print()

        # Performance Analysis
        print("🎯 PERFORMANCE ANALYSIS")

        early_count = metrics.get("early", 0)
        on_time_count = metrics.get("on_time", 0)
        late_count = metrics.get("late", 0)
        overdue_count = metrics.get("overdue", 0)

        total_completed = early_count + on_time_count + late_count
        successful_completion = early_count + on_time_count

        if total_completed > 0:
            success_rate = (successful_completion / total_completed) * 100
            print(f"   ✅ Completion Success Rate: {success_rate:.1f}%")
            print(f"      Successfully completed on or before due date: {successful_completion}/{total_completed}")

        if overdue_count > 0:
            print(f"   ⚠️  Active Risks: {overdue_count} overdue issues require immediate attention")

        if late_count > 0:
            print(f"   📋 Improvement Area: {late_count} issues completed late")

        print()

        # Recommendations
        print("💡 RECOMMENDATIONS")
        if adherence_rate < 60:
            print("   🚨 IMMEDIATE ACTIONS REQUIRED:")
            print("      • Review project planning and estimation processes")
            print("      • Implement more frequent milestone check-ins")
            print("      • Consider workload balancing across team members")
        elif adherence_rate < 80:
            print("   ⚠️  IMPROVEMENT OPPORTUNITIES:")
            print("      • Enhance due date visibility and tracking")
            print("      • Implement early warning systems for at-risk issues")
            print("      • Review resource allocation and capacity planning")
        else:
            print("   ✅ MAINTAIN EXCELLENCE:")
            print("      • Continue current practices and monitoring")
            print("      • Share best practices with other teams")
            print("      • Focus on continuous improvement")

        print()

        # Data Quality Notes
        print("📋 DATA QUALITY NOTES")
        print("   • Analysis based on issues resolved within the specified time period")
        print("   • Only issues with due dates are included in adherence calculations")
        print("   • Adherence rate = (Early + On-time) / Total completed issues")

        if args.output_file:
            print(f"\n📄 Detailed report saved to: {args.output_file}")

        print("=" * 79)
