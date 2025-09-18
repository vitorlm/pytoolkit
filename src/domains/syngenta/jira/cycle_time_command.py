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

from domains.syngenta.jira.cycle_time_formatter import CycleTimeFormatter
from domains.syngenta.jira.cycle_time_service import CycleTimeService
from domains.syngenta.jira.cycle_time_trend_service import CycleTimeTrendService, TrendConfig
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
        parser.add_argument(
            "--output-format",
            type=str,
            choices=["json", "md", "console"],
            default="console",
            help="Output format: json (JSON file), md (Markdown file), console (display only)",
        )
        parser.add_argument(
            "--enable-trending",
            action="store_true",
            help="Enable trending analysis comparing current period against historical baseline.",
        )
        parser.add_argument(
            "--baseline-multiplier",
            type=int,
            default=4,
            help="Multiplier for baseline period calculation (default: 4x current period).",
        )

    @staticmethod
    def main(args: Namespace):
        """
        Main function to execute cycle time analysis.

        Args:
            args (Namespace): Command-line arguments.
        """
        ensure_env_loaded()
        logger = LogManager.get_instance().get_logger("CycleTimeCommand")

        try:
            # Parse inputs
            issue_types = [t.strip() for t in args.issue_types.split(",")]
            priorities = [p.strip() for p in args.priority.split(",")] if args.priority else None

            # Execute analysis through service
            service = CycleTimeService()
            result = service.analyze_cycle_time(
                project_key=args.project_key,
                time_period=args.time_period,
                issue_types=issue_types,
                team=args.team,
                priorities=priorities,
                verbose=args.verbose,
                output_file=args.output_file,
            )

            if result:
                logger.info("Cycle time analysis completed successfully")

                # Execute trending analysis if enabled
                trending_results = None
                if args.enable_trending:
                    logger.info("Starting trending analysis...")
                    try:
                        # Create trend service with custom configuration
                        trend_config = TrendConfig({"baseline_multiplier": args.baseline_multiplier})
                        trend_service = CycleTimeTrendService(trend_config)

                        # Convert current results to trend data format
                        current_trend_data = trend_service.convert_cycle_time_data_to_trend_data(result)

                        # Calculate baseline period
                        metadata = result.get("analysis_metadata", {})
                        start_date = metadata.get("start_date", "")
                        end_date = metadata.get("end_date", "")

                        baseline_start, baseline_end = trend_service.calculate_baseline_period(start_date, end_date)

                        # Analyze historical data for baseline
                        baseline_result = service.analyze_cycle_time(
                            project_key=args.project_key,
                            time_period=f"{baseline_start.strftime('%Y-%m-%d')} to {baseline_end.strftime('%Y-%m-%d')}",
                            issue_types=issue_types,
                            team=args.team,
                            priorities=priorities,
                            verbose=False,  # Don't show verbose for baseline
                            output_file=None,  # Don't save baseline separately
                        )

                        if baseline_result:
                            baseline_trend_data = trend_service.convert_cycle_time_data_to_trend_data(baseline_result)

                            # Calculate trend metrics
                            trend_metrics = trend_service.calculate_trend_metrics(
                                current_trend_data, [baseline_trend_data]
                            )

                            # Detect patterns and generate alerts
                            alerts = trend_service.detect_patterns_and_alerts(trend_metrics, current_trend_data)

                            trending_results = {
                                "trend_metrics": trend_metrics,
                                "alerts": alerts,
                                "baseline_period": {
                                    "start": baseline_start.isoformat(),
                                    "end": baseline_end.isoformat(),
                                    "data": baseline_trend_data,
                                },
                            }

                            logger.info(f"Trending analysis completed with {len(alerts)} alerts")
                        else:
                            logger.warning("Could not analyze baseline data for trending")

                    except Exception as e:
                        logger.error(f"Trending analysis failed: {e}")
                        logger.error(f"Exception type: {type(e).__name__}")
                        logger.error(f"Exception details: {str(e)}")
                        import traceback

                        logger.error(f"Traceback: {traceback.format_exc()}")
                        # Continue with regular analysis even if trending fails

                # Add trending results to main result if available
                if trending_results:
                    result["trending_analysis"] = trending_results

                # For JSON serialization, create a helper function
                def serialize_trending_results(trending_data):
                    """Convert trending results to JSON-serializable format."""
                    trend_metrics_dict = [
                        {
                            "metric_name": tm.metric_name,
                            "current_value": tm.current_value,
                            "baseline_value": tm.baseline_value,
                            "change_percent": tm.change_percent,
                            "trend_direction": tm.trend_direction,
                            "significance": tm.significance,
                            "confidence_level": tm.confidence_level,
                            "trend_slope": tm.trend_slope,
                            "volatility": tm.volatility,
                        }
                        for tm in trending_data["trend_metrics"]
                    ]

                    alerts_dict = [
                        {
                            "severity": alert.severity,
                            "metric": alert.metric,
                            "message": alert.message,
                            "current_value": alert.current_value,
                            "threshold": alert.threshold,
                            "recommendation": alert.recommendation,
                            "priority": getattr(alert, "priority", 0),
                        }
                        for alert in trending_data["alerts"]
                    ]

                    baseline_data_dict = {
                        "avg_cycle_time": trending_data["baseline_period"]["data"].avg_cycle_time,
                        "median_cycle_time": trending_data["baseline_period"]["data"].median_cycle_time,
                        "sle_compliance": trending_data["baseline_period"]["data"].sle_compliance,
                        "throughput": trending_data["baseline_period"]["data"].throughput,
                        "anomaly_rate": trending_data["baseline_period"]["data"].anomaly_rate,
                        "period_start": trending_data["baseline_period"]["data"].period_start.isoformat(),
                        "period_end": trending_data["baseline_period"]["data"].period_end.isoformat(),
                        "total_issues": trending_data["baseline_period"]["data"].total_issues,
                        "issues_with_valid_cycle_time": trending_data["baseline_period"][
                            "data"
                        ].issues_with_valid_cycle_time,
                        "avg_lead_time": trending_data["baseline_period"]["data"].avg_lead_time,
                        "median_lead_time": trending_data["baseline_period"]["data"].median_lead_time,
                    }

                    return {
                        "trend_metrics": trend_metrics_dict,
                        "alerts": alerts_dict,
                        "baseline_period": {
                            "start": trending_data["baseline_period"]["start"],
                            "end": trending_data["baseline_period"]["end"],
                            "data": baseline_data_dict,
                        },
                    }

                # Handle output format
                if args.output_format == "json":
                    import json
                    import os
                    from datetime import datetime

                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    output_file = f"output/cycle-time/cycle_time_{args.project_key}_{timestamp}.json"
                    os.makedirs(os.path.dirname(output_file), exist_ok=True)

                    # Create serializable version for JSON
                    result_for_json = result.copy()
                    if trending_results:
                        result_for_json["trending_analysis"] = serialize_trending_results(trending_results)

                    with open(output_file, "w", encoding="utf-8") as f:
                        json.dump(result_for_json, f, indent=2, ensure_ascii=False, default=str)
                    print(f"✅ JSON report saved to: {output_file}")

                elif args.output_format == "md":
                    import os
                    from datetime import datetime

                    # Use timestamp for consistent file naming
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    output_file = f"output/cycle-time/cycle_time_{args.project_key}_{timestamp}.md"
                    os.makedirs(os.path.dirname(output_file), exist_ok=True)

                    if hasattr(service, "_format_as_markdown"):
                        # Create serializable version for MD
                        result_for_md = result.copy()
                        if trending_results:
                            result_for_md["trending_analysis"] = serialize_trending_results(trending_results)

                        markdown_content = service._format_as_markdown(result_for_md)
                        with open(output_file, "w", encoding="utf-8") as f:
                            f.write(markdown_content)
                        print(f"📄 Markdown report saved to: {output_file}")
                    else:
                        logger.warning("Markdown format not yet implemented")

                else:
                    # Enhanced console display using formatter
                    formatter = CycleTimeFormatter()
                    formatter.display_enhanced_console(result)

                # Save updated result with trending data when trending is enabled
                if args.enable_trending and trending_results:
                    import json
                    import os
                    from datetime import datetime

                    # Generate timestamp for trending-enabled files
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    output_file = f"output/cycle-time/cycle_time_{args.project_key}_{timestamp}.json"
                    os.makedirs(os.path.dirname(output_file), exist_ok=True)

                    # Create serializable version for JSON
                    result_for_json = result.copy()
                    result_for_json["trending_analysis"] = serialize_trending_results(trending_results)

                    with open(output_file, "w", encoding="utf-8") as f:
                        json.dump(result_for_json, f, indent=2, ensure_ascii=False, default=str)
                    logger.info(f"Enhanced analysis with trending saved to: {output_file}")

            else:
                logger.error("Cycle time analysis failed")
                exit(1)

        except Exception as e:
            error_str = str(e)
            if "400" in error_str and ("does not exist for the field 'type'" in error_str):
                logger.error(f"Invalid issue type for project {args.project_key}: {e}")
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
                logger.error(f"Failed to execute cycle time analysis: {e}")
            exit(1)
