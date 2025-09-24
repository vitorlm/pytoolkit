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

1. Analyze last 7 days (anchored on a date):
   python src/main.py syngenta jira cycle-time --project-key "CWS" \
   --end-date "2025-09-21" --window-days 7

2. Analyze last 14 days with specific issue types:
   python src/main.py syngenta jira cycle-time --project-key "CWS" \
   --end-date "2025-09-21" --window-days 14 --issue-types "Story,Task"

3. Analyze last 30 days:
   python src/main.py syngenta jira cycle-time --project-key "CWS" \
   --end-date "2025-09-21" --window-days 30

4. Analyze specific date range (use explicit anchor + window):
   python src/main.py syngenta jira cycle-time --project-key "CWS" \
   --end-date "2025-06-22" --window-days 14

5. Analyze a single day:
   python src/main.py syngenta jira cycle-time --project-key "CWS" \
   --end-date "2025-06-15" --window-days 1

6. Analyze with team filter:
   python src/main.py syngenta jira cycle-time --project-key "CWS" \
   --end-date "2025-09-21" --window-days 7 --team "Catalog"

7. Export results to file:
   python src/main.py syngenta jira cycle-time --project-key "CWS" \
   --end-date "2025-09-21" --window-days 7 --output-format md

TIME WINDOW (ANCHOR + WINDOW-DAYS):
- Use `--end-date YYYY-MM-DD` to anchor the analysis
- Use `--window-days N` to define the lookback window (e.g., 7, 14, 30)

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
        # New time window options (following Net Flow pattern)
        parser.add_argument(
            "--end-date",
            type=str,
            required=False,
            help=(
                "Anchor date in YYYY-MM-DD. If provided with --window-days, "
                "overrides --time-period. Defaults to today when not set."
            ),
        )
        parser.add_argument(
            "--window-days",
            type=int,
            required=False,
            default=7,
            help="Window size in days counting backwards from end-date (default: 7).",
        )
        # Removed deprecated --time-period in favor of --end-date + --window-days
        parser.add_argument(
            "--issue-types",
            type=str,
            required=False,
            default="Story,Task,Bug,Epic",
            help=(
                "Comma-separated list of issue types to analyze (default: 'Story,Task,Bug,Epic')."
            ),
        )
        parser.add_argument(
            "--team",
            type=str,
            required=False,
            help="Filter by team name using Squad[Dropdown] field (optional).",
        )
        parser.add_argument(
            "--include-subtasks",
            action="store_true",
            help="Include subtasks in the analysis (default: excluded).",
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
            choices=["json", "md"],
            default="console",
            help="Output format: json (JSON file), md (Markdown file), console (display only)",
        )
        parser.add_argument(
            "--extended",
            action="store_true",
            help="Enable extended analysis (baseline/trending, alerts).",
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
            include_subtasks = bool(getattr(args, "include_subtasks", False))

            # Determine time window (prefer --end-date + --window-days)
            from datetime import date, datetime, timedelta
            computed_time_period = None

            if args.end_date:
                try:
                    anchor = datetime.fromisoformat(args.end_date).date()
                except ValueError:
                    raise ValueError("Invalid --end-date. Use YYYY-MM-DD format.")
            else:
                anchor = date.today()

            if args.end_date or args.window_days:
                # Compute start/end from anchor/window
                window_days = int(getattr(args, "window_days", 7))
                start = anchor - timedelta(days=max(window_days - 1, 0))
                end = anchor
                computed_time_period = f"{start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}"

            # Execute analysis through service
            service = CycleTimeService()
            result = service.analyze_cycle_time(
                project_key=args.project_key,
                time_period=(computed_time_period or f"{(date.today()-timedelta(days=6)).strftime('%Y-%m-%d')} to {date.today().strftime('%Y-%m-%d')}") ,
                issue_types=issue_types,
                team=args.team,
                priorities=priorities,
                verbose=args.verbose,
                include_subtasks=include_subtasks,
                output_file=args.output_file,
            )

            if result:
                logger.info("Cycle time analysis completed successfully")

                # Execute trending analysis if enabled
                # Determine extended mode (Net Flow pattern)
                extended = bool(getattr(args, "extended", False))

                trending_results = None
                if extended:
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

                        # Show user-friendly error message
                        print("\n⚠️  TRENDING ANALYSIS ERROR")
                        print(f"Error: {e}")
                        print("Continuing with regular cycle time analysis...")

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
                            "normalized_baseline": getattr(tm, "normalized_baseline", tm.baseline_value),
                            "is_normalized": getattr(tm, "is_normalized", False),
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

                # Always print a concise Executive Summary to console
                try:
                    CycleTimeCommand._print_executive_summary(result)
                except Exception as _summary_err:
                    logger.warning(f"Failed to print executive summary: {_summary_err}")

                # Handle output format (standardized via OutputManager)
                from utils.output_manager import OutputManager
                from datetime import datetime as _dt
                # Save into per-day folder (cycle-time_YYYYMMDD)
                sub_dir = f"cycle-time_{_dt.now().strftime('%Y%m%d')}"
                file_basename = f"cycle_time_{args.project_key}"

                if args.output_format == "json":
                    # Create serializable version for JSON
                    result_for_json = result.copy()
                    if trending_results:
                        result_for_json["trending_analysis"] = serialize_trending_results(trending_results)
                    # Precompute path so we can show it right under the summary
                    output_path = OutputManager.get_output_path(sub_dir, file_basename, "json")
                    print(f"\nOutput file:\n- {output_path}")
                    OutputManager.save_json_report(
                        result_for_json,
                        sub_dir,
                        file_basename,
                        output_path=output_path,
                    )
                    # expose path in result for downstream consumers
                    result["output_file"] = output_path
                    print("✅ Detailed report saved in JSON format")

                elif args.output_format == "md":
                    if hasattr(service, "_format_as_markdown"):
                        # Create serializable version for MD
                        result_for_md = result.copy()
                        if trending_results:
                            result_for_md["trending_analysis"] = serialize_trending_results(trending_results)

                        markdown_content = service._format_as_markdown(result_for_md)
                        # Precompute path so we can show it right under the summary
                        output_path = OutputManager.get_output_path(sub_dir, file_basename, "md")
                        print(f"\nOutput file:\n- {output_path}")
                        OutputManager.save_markdown_report(
                            markdown_content,
                            sub_dir,
                            file_basename,
                            output_path=output_path,
                        )
                        # expose path in result for downstream consumers
                        result["output_file"] = output_path
                        print("📄 Detailed report saved in MD format")
                    else:
                        logger.warning("Markdown format not yet implemented")

                else:
                    # Enhanced console display using formatter
                    try:
                        formatter = CycleTimeFormatter()
                        formatter.display_enhanced_console(result)
                    except Exception as formatter_error:
                        logger.error(f"Error in formatter display: {formatter_error}")
                        logger.error(f"Formatter error type: {type(formatter_error).__name__}")
                        import traceback
                        logger.error(f"Formatter traceback: {traceback.format_exc()}")

                        # Fallback: show basic info without fancy formatting
                        print("\n❌ FORMATTING ERROR OCCURRED")
                        print(f"Error: {formatter_error}")
                        print("\nBasic Analysis Results:")
                        print(f"Project: {result.get('analysis_metadata', {}).get('project_key', 'Unknown')}")
                        print(f"Total Issues: {result.get('metrics', {}).get('total_issues', 0)}")
                        print(f"Average Cycle Time: {result.get('metrics', {}).get('average_cycle_time_hours', 0):.1f}h")

                        if trending_results:
                            print("\nTrending data is available but could not be displayed due to formatting error.")
                            print("Check logs for details or use --output-format json for raw data.")

                # (Removed duplicate OutputManager saving block to avoid double-formatting)

            else:
                logger.error("Cycle time analysis failed")
                exit(1)

        except Exception as e:
            error_str = str(e)
            logger.error(f"COMMAND FAILED: {e}")
            logger.error(f"Exception type: {type(e).__name__}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")

            if "400" in error_str and ("does not exist for the field 'type'" in error_str):
                logger.error(f"Invalid issue type for project {args.project_key}: {e}")
                print(f"\n{'=' * 50}")
                print("❌ ERROR: Invalid Issue Type Detected")
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
                print(f"\n{'=' * 50}")
                print("❌ UNEXPECTED ERROR OCCURRED")
                print("=" * 50)
                print(f"Error: {e}")
                print(f"Error Type: {type(e).__name__}")
                print("\nThis appears to be an unexpected error. Please check the logs for more details:")
                print("Check: logs/ directory for detailed error information")
                print("\nIf the problem persists, try:")
                print("1. Run with --verbose for more details")
                print("2. Use --output-format json to see raw data")
                print("3. Check if all parameters are correct")
                print("=" * 50)
                logger.error(f"Failed to execute cycle time analysis: {e}")
            exit(1)

    @staticmethod
    def _print_executive_summary(result: dict) -> None:
        """Print a brief Executive Summary to console regardless of output mode."""
        metadata = result.get("analysis_metadata", {})
        metrics = result.get("metrics", {})

        project = metadata.get("project_key", "-")
        start = metadata.get("start_date", "")
        end = metadata.get("end_date", "")
        period = metadata.get("time_period", "")

        try:
            start_disp = start[:10]
            end_disp = end[:10]
            if "T" in start:
                start_disp = start[:10]
            if "T" in end:
                end_disp = end[:10]
        except Exception:
            start_disp = start
            end_disp = end

        header = f"⏱️ CYCLE TIME - {project} ({start_disp} to {end_disp})"
        print("\n" + "=" * len(header))
        print(header)
        if end_disp:
            print(f"(Anchored on: {end_disp})")
        print("=" * len(header))

        median_ct = metrics.get("median_cycle_time_hours", 0.0)
        avg_ct = metrics.get("average_cycle_time_hours", 0.0)
        total_issues = metrics.get("total_issues", 0)
        valid_ct = metrics.get("issues_with_valid_cycle_time", 0)
        anomalies = metrics.get("zero_cycle_time_anomalies", 0)

        print("\nEXECUTIVE SUMMARY:")
        print(f"- Median Cycle Time: {median_ct:.1f}h")
        print(f"- Average Cycle Time: {avg_ct:.1f}h")
        print(f"- Issues (valid/total): {valid_ct}/{total_issues}")
        if anomalies:
            print(f"- Zero-cycle anomalies: {anomalies}")
        print("=" * len(header))
