"""
JIRA Issue Resolution Time Analysis Command

This command analyzes issue resolution time for bugs and support tickets with improved
SLA calculation logic and clean output structure.

FUNCTIONALITY:
- Fetch issues that were resolved within a specified time period
- Filter by issue types (bug, support, etc.)
- Group results by priority level (P1, P2, etc.)
- Calculate median, P95, and average resolution times with improved accuracy
- Support filtering by fixversion, squad, and assignee
- Clean, decision-oriented output with optional raw data inclusion

RESOLUTION TIME CALCULATION:
- Bug and Support issues: Time from issue creation to resolution (created_date → resolved_date)
- Other issue types: Time from first "In Progress" status to resolution (start_date → resolved_date)
- This provides more accurate SLA measurements for customer-facing issues

OUTPUT MODES:
- Default (clean): Essential metrics only - issue_type, priority, count, median_days,
  p90_days, p95_days, std_dev, p95_trimmed_days, suggested_sla_days, sla_risk_level
- Raw data mode (--include-raw): Includes debugging fields, full issue lists,
  resolution buckets, confidence intervals, and validation warnings

USAGE EXAMPLES:

1. Analyze last week's bugs and support tickets:
   python src/main.py syngenta jira calculate-resolution-time --project-key "CWS"
   --time-period "last-week"

2. Analyze specific date range with priority grouping:
   python src/main.py syngenta jira calculate-resolution-time --project-key "CWS"
   --time-period "2025-06-09 to 2025-06-22" --issue-types "Bug,Support"

3. Filter by squad and fixversion:
   python src/main.py syngenta jira calculate-resolution-time --project-key "CWS"
   --time-period "last-month" --squad "Catalog" --fixversion "Q1C1"

4. Export results to CSV with enhanced statistics:
   python src/main.py syngenta jira calculate-resolution-time --project-key "CWS"
   --time-period "last-week" --output-file "resolution_time_report.csv"

5. Analyze with outlier exclusion for more reliable SLA recommendations:
   python src/main.py syngenta jira calculate-resolution-time --project-key "CWS"
   --time-period "last-month" --exclude-outliers

6. Generate charts with the analysis:
   python src/main.py syngenta jira calculate-resolution-time --project-key "CWS"
   --time-period "last-month" --generate-charts

7. Generate specific chart types only:
   python src/main.py syngenta jira calculate-resolution-time --project-key "CWS"
   --time-period "last-month" --generate-charts
   --chart-types "metrics_comparison,sla_comparison,trends_over_time"

8. Use different outlier detection strategies:
   python src/main.py syngenta jira calculate-resolution-time --project-key "CWS"
   --time-period "last-month" --exclude-outliers --outlier-strategy "iqr"

9. Include raw data for debugging and detailed analysis:
   python src/main.py syngenta jira calculate-resolution-time --project-key "CWS"
   --time-period "last-month" --include-raw

10. Generate clean output for decision-making (default behavior):
    python src/main.py syngenta jira calculate-resolution-time --project-key "CWS"
    --time-period "last-month"

TIME PERIOD OPTIONS:
- last-week: Issues from the last 7 days
- last-2-weeks: Issues from the last 14 days
- last-month: Issues from the last 30 days
- N-days: Issues from the last N days (e.g., "15-days")
- YYYY-MM-DD to YYYY-MM-DD: Specific date range
- YYYY-MM-DD: Single specific date

METRICS CALCULATED:
- Total number of closed issues per priority and type
- Median resolution time (days)
- P90, P95 resolution time (days)
- P95 trimmed (with flexible outlier exclusion)
- Standard deviation
- Robust suggested SLA days (updated formula: max of P95, median+std, P95_trimmed)
- SLA risk level assessment
- Outlier analysis with multiple detection strategies
- Resolution time buckets for histogram analysis (raw data mode)
- Optional confidence intervals for P95 (bootstrap method, raw data mode)
- Statistical validation checks (raw data mode)

ENHANCED FEATURES:
- Improved resolution time calculation:
  * Bug and Support types: calculated from created_date to resolved_date
  * Other types: calculated from start_date to resolved_date (backward compatible)
- Updated SLA suggestion formula: max(P95, median+std_dev, P95_trimmed)
- Clean, decision-oriented output structure by default
- Optional raw data inclusion with --include-raw flag
- Flexible outlier detection strategies (trim, IQR, z-score)
- Resolution time trend analysis over time
- SLA vs actual metrics comparison charts
- Statistical validation and consistency checks
- Enhanced CSV output with all metrics and buckets
"""

from argparse import ArgumentParser, Namespace
import hashlib
import os
from datetime import datetime

from domains.syngenta.jira.issue_resolution_time_service import (
    IssueResolutionTimeService,
)
from domains.syngenta.jira.issue_resolution_time_chart_service import (
    IssueResolutionTimeChartService,
)
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager


class CalculateResolutionTimeCommand(BaseCommand):
    """Command to analyze issue resolution time for bugs and support tickets."""

    @staticmethod
    def get_name() -> str:
        return "calculate-resolution-time"

    @staticmethod
    def get_description() -> str:
        return "Analyze issue resolution time for bugs and support tickets grouped by priority."

    @staticmethod
    def get_help() -> str:
        return (
            "This command analyzes issue resolution time by fetching resolved issues "
            "within a time period and calculating statistics (median, P95, average) "
            "grouped by issue type (bug/support) and priority level."
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
            default="Bug,Support",
            help="Comma-separated list of issue types to include (default: 'Bug,Support').",
        )
        parser.add_argument(
            "--squad",
            type=str,
            required=False,
            help="Filter by squad/team name (e.g., 'Catalog', 'Platform').",
        )
        parser.add_argument(
            "--fixversion",
            type=str,
            required=False,
            help="Filter by fix version (e.g., 'Q1C1', 'Q2C2').",
        )
        parser.add_argument(
            "--assignee",
            type=str,
            required=False,
            help="Filter by assignee email or username.",
        )
        parser.add_argument(
            "--output-file",
            type=str,
            required=False,
            help="Optional file path to save the results (JSON or CSV based on extension).",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Enable verbose output showing individual issue details.",
        )
        parser.add_argument(
            "--exclude-outliers",
            action="store_true",
            help="Exclude top 2%% outliers when calculating trimmed percentiles and SLAs.",
        )
        parser.add_argument(
            "--generate-charts",
            action="store_true",
            help="Generate visualization charts for the resolution time analysis.",
        )
        parser.add_argument(
            "--chart-types",
            type=str,
            required=False,
            help=(
                "Comma-separated list of chart types to generate. "
                "Options: 'metrics_comparison', 'count_distribution', "
                "'variability_analysis', 'boxplot', 'sla_comparison', "
                "'trends_over_time', 'histogram'. "
                "If not specified, all charts are generated when --generate-charts is used."
            ),
        )
        parser.add_argument(
            "--outlier-strategy",
            type=str,
            required=False,
            default="trim",
            choices=["none", "trim", "iqr", "zscore"],
            help=(
                "Strategy for outlier detection when --exclude-outliers is used. "
                "Options: 'none' (no outlier removal), 'trim' (remove top 2%%), "
                "'iqr' (Interquartile Range method), 'zscore' (remove z-score > 3). "
                "Default: 'trim'."
            ),
        )
        parser.add_argument(
            "--include-raw",
            action="store_true",
            help="Include raw issue data and debugging fields in output for troubleshooting.",
        )

    @staticmethod
    def main(args: Namespace):
        """
        Main function to execute issue resolution time analysis.

        Args:
            args (Namespace): Command-line arguments.
        """
        # Ensure environment variables are loaded
        ensure_env_loaded()

        logger = LogManager.get_instance().get_logger("CalculateResolutionTimeCommand")

        try:
            # Parse issue types
            issue_types = [t.strip() for t in args.issue_types.split(",")]

            # Create unique run identifier based on parameters and timestamp
            run_params = {
                "project_key": args.project_key,
                "time_period": args.time_period,
                "issue_types": args.issue_types,
                "squad": args.squad,
                "fixversion": args.fixversion,
                "assignee": args.assignee,
                "exclude_outliers": args.exclude_outliers,
            }

            # Create timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Create parameter hash for uniqueness
            param_string = "_".join(
                [f"{k}={v}" for k, v in run_params.items() if v is not None]
            )
            param_hash = hashlib.md5(param_string.encode()).hexdigest()[:8]

            # Create unique run identifier
            run_id = f"{timestamp}_{args.project_key}_{param_hash}"

            # Initialize service
            service = IssueResolutionTimeService()

            # Modify output file to include run_id if not specified
            output_file = args.output_file
            if not output_file:
                from utils.output_manager import OutputManager

                output_file = OutputManager.get_output_path(
                    "issue-resolution-time", f"resolution_time_{run_id}"
                )

            # Run resolution time analysis
            result = service.analyze_resolution_time(
                project_key=args.project_key,
                time_period=args.time_period,
                issue_types=issue_types,
                squad=args.squad,
                fixversion=args.fixversion,
                assignee=args.assignee,
                verbose=args.verbose,
                output_file=output_file,
                exclude_outliers=args.exclude_outliers,
                outlier_strategy=args.outlier_strategy,
                include_raw=args.include_raw,
            )

            # Add run information to result
            if result:
                result["run_info"] = {
                    "run_id": run_id,
                    "timestamp": timestamp,
                    "parameters": run_params,
                    "parameter_hash": param_hash,
                }

            # Generate charts first if requested
            if args.generate_charts:
                CalculateResolutionTimeCommand._generate_charts(result, args, run_id)

            # Print summary results after charts (or immediately if no charts)
            CalculateResolutionTimeCommand._print_summary_results(
                result, args, issue_types
            )

        except Exception as e:
            logger.error(f"Failed to analyze resolution time: {e}")
            exit(1)

    @staticmethod
    def _print_summary_results(result: dict, args: Namespace, issue_types: list):
        """
        Print the summary results of the resolution time analysis.

        Args:
            result (dict): Analysis results
            args (Namespace): Command arguments
            issue_types (list): List of issue types analyzed
        """
        if not result or "summary" not in result:
            return

        summary = result["summary"]
        print("\n" + "=" * 80)
        print("ISSUE RESOLUTION TIME ANALYSIS")
        print("=" * 80)
        print(f"Project: {args.project_key}")
        print(f"Time Period: {args.time_period}")
        print(f"Issue Types: {', '.join(issue_types)}")
        if args.squad:
            print(f"Squad: {args.squad}")
        if args.fixversion:
            print(f"Fix Version: {args.fixversion}")
        if args.assignee:
            print(f"Assignee: {args.assignee}")
        if args.exclude_outliers:
            actual_strategy = result.get("query_info", {}).get(
                "outlier_strategy", args.outlier_strategy
            )
            print(f"Outlier Exclusion: Enabled (strategy: {actual_strategy})")
        print()

        total_issues = summary.get("total_issues", 0)
        print(f"Total Issues Analyzed: {total_issues}")

        if total_issues == 0:
            print("No issues found matching the criteria.")
            return

        # Print results by issue type and priority
        if "by_type_and_priority" in result:
            for issue_type, priorities in result["by_type_and_priority"].items():
                print(f"\n📊 {issue_type.upper()} ISSUES")
                print("-" * 50)

                if not priorities:
                    print("  No issues found for this type.")
                    continue

                # Print header
                print(
                    f"{'Priority':<12} {'Count':<6} {'Median':<8} {'P80':<8} "
                    f"{'P90':<8} {'P95':<8} {'SLA':<6} {'Max':<10} {'SLA%':<7}"
                )
                print(
                    f"{'=' * 12} {'=' * 6} {'=' * 8} {'=' * 8} "
                    f"{'=' * 8} {'=' * 8} {'=' * 6} {'=' * 10} {'=' * 7}"
                )

                # Print each priority
                for priority, stats in priorities.items():
                    count = stats.get("count", 0)
                    median = stats.get("median_days", 0)
                    p80 = stats.get("p80_days", 0)
                    p90 = stats.get("p90_days", 0)
                    p95 = stats.get("p95_days", 0)
                    sla = stats.get("suggested_sla_days", 0)
                    max_days = stats.get("max_days", 0)
                    sla_pct = stats.get("sla_compliance_percentage", 0)

                    print(
                        f"{priority:<12} {count:<6} {median:<8.1f} {p80:<8.1f} "
                        f"{p90:<8.1f} {p95:<8.1f} {sla:<6} {max_days:<10.1f} {sla_pct:>6.1f}%"
                    )

        # Print overall statistics
        if "overall_stats" in result:
            overall = result["overall_stats"]
            print("\n📈 OVERALL STATISTICS")
            print("-" * 40)
            print(f"Total Issues: {overall.get('total_issues', 0)}")
            median_time = overall.get("median_days", 0)
            print(f"Overall Median Resolution: {median_time:.1f} days")
            p90_time = overall.get("p90_days", 0)
            print(f"Overall P90 Resolution: {p90_time:.1f} days")
            p95_time = overall.get("p95_days", 0)
            print(f"Overall P95 Resolution: {p95_time:.1f} days")
            suggested_sla = overall.get("suggested_sla_days", 0)
            print(f"Overall Suggested SLA: {suggested_sla} days")
            sla_pct = overall.get("sla_compliance_percentage", 0)
            print(f"Overall SLA Compliance: {sla_pct:.1f}%")

        # Print outlier summary if available
        if "outlier_summary" in result:
            print("\n🚫 OUTLIER SUMMARY")
            print("-" * 40)
            for issue_type, summary_data in result["outlier_summary"].items():
                total = summary_data.get("total_issues", 0)
                excluded = summary_data.get("outliers_excluded", 0)
                percentage = summary_data.get("outlier_percentage", 0)
                threshold = summary_data.get("outlier_threshold_days", 0)
                print(
                    f"{issue_type}: {excluded}/{total} outliers excluded "
                    f"({percentage:.1f}%) above {threshold:.1f} days"
                )

        # Show file export information
        if args.output_file:
            print(f"\n💾 Results exported to: {args.output_file}")

        print("\n" + "=" * 80)

    @staticmethod
    def _generate_charts(result: dict, args: Namespace, run_id: str):
        """
        Generate charts for the resolution time analysis.

        Args:
            result (dict): Analysis results
            args (Namespace): Command arguments
            run_id (str): Unique run identifier
        """
        logger = LogManager.get_instance().get_logger("CalculateResolutionTimeCommand")

        try:
            # Determine chart output path with unique run ID (without using OutputManager
            # to avoid automatic .json extension)
            from utils.file_manager import FileManager
            from utils.output_manager import OutputManager

            # Use OutputManager's project root detection logic
            base_output_dir = os.path.join(
                OutputManager.PROJECT_ROOT, "output", "issue-resolution-time", "charts"
            )
            chart_output_path = os.path.join(base_output_dir, run_id)
            FileManager.create_folder(chart_output_path)

            # Initialize chart service
            chart_service = IssueResolutionTimeChartService(
                output_path=chart_output_path
            )

            # Parse chart types if specified
            chart_types = None
            if args.chart_types:
                chart_types = [t.strip() for t in args.chart_types.split(",")]

            # Generate charts
            stats_data = result.get("by_type_and_priority", {})
            raw_issues_data = result.get("issues", [])

            chart_service.plot_all_charts(
                stats_by_type_priority=stats_data,
                raw_issues_data=raw_issues_data,
                chart_types=chart_types,
            )

            print(f"\n📊 Charts generated and saved to: {chart_output_path}")
            print(f"Run ID: {run_id}")

        except Exception as e:
            logger.error(f"Failed to generate charts: {e}")
            print(f"Warning: Chart generation failed: {e}")
