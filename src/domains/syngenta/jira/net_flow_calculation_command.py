"""
JIRA Net Flow Calculation Command

This command calculates net flow metrics by analyzing arrival rate (issues created)
versus throughput (issues completed) for specified time periods.

It generates a "Flow Health Scorecard" with a rolling 4-week trend analysis.

USAGE EXAMPLES:

1. Basic net flow scorecard for the last week:
   python src/main.py syngenta jira net-flow-calculation --project-key "CWS" --time-period "last-week"

2. Scorecard for a specific team and issue types:
   python src/main.py syngenta jira net-flow-calculation --project-key "CWS" \
   --time-period "last-week" --team "Catalog" --issue-types "Story,Bug"

3. Verbose output with issue details for the current week:
   python src/main.py syngenta jira net-flow-calculation --project-key "CWS" \
   --time-period "last-week" --verbose

SCORECARD INTERPRETATION:
- Net Flow: Difference between work arriving and work completed.
  - Positive: Backlog may be growing.
  - Negative: Backlog may be shrinking.
- Rolling 4 Weeks Trend: Shows the pattern of Net Flow over time, helping to
  distinguish a one-off event from a persistent trend.
- Flow Ratio: The ratio of throughput to arrival, indicating efficiency.
"""

from argparse import ArgumentParser, Namespace
from datetime import datetime, date

from domains.syngenta.jira.net_flow_calculation_service import NetFlowCalculationService
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager


class NetFlowCalculationCommand(BaseCommand):
    """Command to generate a Net Flow Health Scorecard for JIRA projects."""

    @staticmethod
    def get_name() -> str:
        return "net-flow-calculation"

    @staticmethod
    def get_description() -> str:
        return "Generate a Net Flow Health Scorecard with a rolling 4-week trend."

    @staticmethod
    def get_help() -> str:
        return (
            "This command generates a comprehensive Net Flow Health Scorecard. It analyzes "
            "the arrival rate vs. throughput, provides a rolling 4-week trend, and offers "
            "actionable insights into your project's flow dynamics."
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
            "--end-date",
            type=str,
            default=date.today().isoformat(),
            help=(
                "The anchor date for the analysis, in YYYY-MM-DD format. The report will analyze the "
                "4 weeks leading up to this date. Defaults to today."
            ),
        )
        parser.add_argument(
            "--issue-types",
            type=str,
            required=False,
            default="Story,Task,Bug,Epic",
            help="Comma-separated list of issue types to analyze (default: 'Story,Task,Bug,Epic').",
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
            "--include-subtasks",
            action="store_true",
            help="Include subtasks in the analysis (default: excluded).",
        )
        parser.add_argument(
            "--output-format",
            type=str,
            required=False,
            help="Optional output format: 'json' or 'md' (default: console only).",
            choices=["json", "md"],
            default="console",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Enable verbose output with detailed issue information for the current week.",
        )

        # Statistical Analysis Flags
        parser.add_argument(
            "--extended",
            action="store_true",
            help="Enable extended statistical analysis (includes CI, EWMA, volatility, segments, and alerts).",
        )
        parser.add_argument(
            "--enable-ci",
            action="store_true",
            help="Enable bootstrap confidence intervals for Net Flow.",
        )
        parser.add_argument(
            "--enable-ewma",
            action="store_true",
            help="Enable EWMA trend analysis for Flow Ratio.",
        )
        parser.add_argument(
            "--enable-cusum",
            action="store_true",
            help="Enable CUSUM shift detection (requires --enable-ewma).",
        )
        parser.add_argument(
            "--cv-window",
            type=int,
            default=8,
            help="Rolling window size for coefficient of variation calculation (default: 8 weeks).",
        )
        parser.add_argument(
            "--alpha",
            type=float,
            default=0.2,
            help="EWMA smoothing parameter (default: 0.2).",
        )
        parser.add_argument(
            "--active-devs",
            type=int,
            help="Number of active developers for normalized metrics.",
        )
        parser.add_argument(
            "--testing-threshold-days",
            type=float,
            default=7.0,
            help="Threshold for testing bottleneck alert (default: 7.0 days).",
        )

    @staticmethod
    def main(args: Namespace):
        """
        Main function to execute scorecard generation.
        """
        ensure_env_loaded()
        logger = LogManager.get_instance().get_logger("NetFlowCalculationCommand")

        try:
            issue_types = [t.strip() for t in args.issue_types.split(",")]
            service = NetFlowCalculationService()

            # Determine statistical analysis settings
            extended = getattr(args, 'extended', False)
            enable_ci = getattr(args, 'enable_ci', False)
            enable_ewma = getattr(args, 'enable_ewma', False)
            enable_cusum = getattr(args, 'enable_cusum', False)

            # Parse teams (supports repeated flags and comma-separated lists)
            raw_teams = getattr(args, "teams", []) or []
            teams = [t.strip() for entry in raw_teams for t in str(entry).split(",") if t.strip()] or None

            # Generate the scorecard
            scorecard = service.generate_net_flow_scorecard(
                project_key=args.project_key,
                end_date=args.end_date,
                issue_types=issue_types,
                teams=teams,
                include_subtasks=args.include_subtasks,
                output_format=args.output_format,
                verbose=args.verbose,
                # Statistical parameters
                enable_statistical_analysis=extended,
                enable_ci=enable_ci or extended,
                enable_ewma=enable_ewma or extended,
                enable_cusum=enable_cusum,
                cv_window=getattr(args, 'cv_window', 8),
                alpha=getattr(args, 'alpha', 0.2),
                active_devs=getattr(args, 'active_devs', None),
                testing_threshold_days=getattr(args, 'testing_threshold_days', 7.0),
            )

            if scorecard:
                logger.info("Net Flow Scorecard generated successfully")
                NetFlowCalculationCommand._print_scorecard(scorecard, args)
                if args.output_format in ["json", "md"]:
                    print(f"\nDetailed scorecard saved in {args.output_format.upper()} format")
            else:
                logger.error("Net Flow Scorecard generation failed")
                exit(1)

        except Exception as e:
            logger.error(f"Failed to generate scorecard: {e}", exc_info=True)
            error_str = str(e)
            if "400" in error_str and "Invalid request payload" in error_str:
                print("\n❌ Jira API Error (400 - Bad Request):")
                print("   The request to the Jira API was malformed. Check the JQL and requested fields.")
                print(f"\n   Error Details: {error_str}")
            else:
                print(f"\n❌ An unexpected error occurred: {e}")
            exit(1)

    @staticmethod
    def _print_scorecard(scorecard: dict, args: Namespace):
        """Print the net flow scorecard to the console."""
        metadata = scorecard.get("metadata", {})
        current_week = scorecard.get("current_week", {})
        trend = scorecard.get("rolling_4_weeks_trend", [])

        start_date = datetime.fromisoformat(metadata.get("start_date", ""))
        end_date = datetime.fromisoformat(metadata.get("end_date", ""))
        week_number = metadata.get("week_number")
        anchor_date = metadata.get("anchor_date")

        header = (
            f"🌊 NET FLOW Health - Week {week_number} ({start_date.strftime('%b %d')}-{end_date.strftime('%b %d')})"
        )
        print("\n" + "=" * len(header))
        print(header)
        print(f"(Anchored on: {anchor_date})")
        print("=" * len(header))

        # NET FLOW ANALYSIS
        print("\nEXECUTIVE SUMMARY:")
        net_flow = current_week.get("net_flow", 0)
        flow_ratio = current_week.get("flow_ratio", 0)
        status_icon, status_text = NetFlowCalculationCommand._get_net_flow_status(net_flow)

        # Basic metrics
        print(f"- Net Flow:     {net_flow:+} {status_icon} ({status_text})")

        # Statistical signal if available
        if "statistical_signal" in scorecard:
            signal = scorecard["statistical_signal"]
            ci_low = signal["net_flow_ci_low"]
            ci_high = signal["net_flow_ci_high"]
            signal_label = signal["signal_label"]
            print(f"- 95% CI:       [{ci_low:.1f}, {ci_high:.1f}] → {signal_label}")

        print(f"- Flow Ratio:   {flow_ratio:.0f}% (target: >85%)")

        # EWMA if available
        if "trend_analysis" in scorecard:
            trend_analysis_data = scorecard["trend_analysis"]
            ewma_ratio = trend_analysis_data["ewma_flow_ratio"]
            direction = trend_analysis_data["trend_direction"]
            print(f"- EWMA Ratio:   {ewma_ratio:.0f}% {direction}")

        print(f"- Arrival:      {current_week.get('arrival_rate', 0)} issues")
        print(f"- Throughput:   {current_week.get('throughput', 0)} issues")

        # Show output file path (when available)
        output_file = scorecard.get("output_file")
        if output_file:
            print("\nOutput file:")
            print(f"- {output_file}")

        # Volatility metrics if available
        if "volatility_metrics" in scorecard:
            vol_metrics = scorecard["volatility_metrics"]
            print(f"- Arrival CV:   {vol_metrics['arrivals_cv']:.0f}% (target: <30%)")
            print(f"- Stability:    {vol_metrics['stability_index']:.0f}%")

        # Flow debt if available
        if "flow_debt" in scorecard:
            print(f"- Flow Debt:    {scorecard['flow_debt']} (QTD)")

        # Status determination
        if "alerts" in scorecard:
            triggered_alerts = [a for a in scorecard["alerts"] if a["triggered"]]
            if triggered_alerts:
                print(f"- Status:       🚨 AT RISK ({len(triggered_alerts)} alerts)")
            else:
                print("- Status:       ✅ BALANCED")
        else:
            flow_status = current_week.get("flow_status", "UNKNOWN")
            status_icon = NetFlowCalculationCommand._get_status_icon(flow_status)
            print(f"- Status:       {status_icon} {flow_status}")

        # FLOW PATTERNS
        print("\nFLOW PATTERNS:")
        arrival_volatility = current_week.get("arrival_volatility", 0)
        print(f"- Arrival Volatility: {arrival_volatility:.0f}%")

        # ROLLING 4 WEEKS TREND
        if trend and len(trend) > 0:
            print("\nROLLING 4 WEEKS TREND:")
            trend_indicator = ""
            if len(trend) > 1:
                try:
                    previous_net_flow = trend[-2]["net_flow"]
                    if net_flow < previous_net_flow:
                        trend_indicator = "✅ (improving)"
                    elif net_flow > previous_net_flow:
                        trend_indicator = "🚨 (worsening)"
                    else:
                        trend_indicator = "(stable)"
                except (IndexError, KeyError):
                    trend_indicator = ""

            for i, week_data in enumerate(trend):
                try:
                    # Ensure week_data is a dict
                    if isinstance(week_data, str):
                        print(f"  Warning: week_data is string: {week_data}")
                        continue

                    week_num = week_data.get("week_number", "N/A")
                    nf = week_data.get("net_flow", 0)
                    arrival_rate = week_data.get("arrival_rate", 0)
                    throughput_rate = week_data.get("throughput", 0)

                    icon, _ = NetFlowCalculationCommand._get_net_flow_status(nf)
                    is_last_week = i == len(trend) - 1
                    indicator = trend_indicator if is_last_week else ""
                    print(f"- Week {week_num}: Net Flow {nf:+} {icon} {indicator}")
                    print(f"  (Arrival: {arrival_rate}, Throughput: {throughput_rate})")
                except Exception as e:
                    print(f"  Error displaying week {i}: {e}")
                    continue

        # FLOW EFFICIENCY
        flow_efficiency = current_week.get("flow_efficiency", 0)
        print(f"\nFLOW EFFICIENCY: {flow_efficiency:.0f}% (target: >40%)")

        # CONSTRAINT ANALYSIS
        bottleneck = current_week.get("primary_bottleneck", "N/A")
        print("\nCONSTRAINT ANALYSIS:")
        print(f"- Primary Bottleneck: {bottleneck}")

        # Segments by Issue Type
        if "segments" in scorecard and scorecard["segments"]:
            print("\nDIAGNOSTICS - BY ISSUE TYPE:")
            for segment in scorecard["segments"]:
                net_flow = segment["net_flow"]
                flow_icon, _ = NetFlowCalculationCommand._get_net_flow_status(net_flow)
                print(f"- {segment['segment_name']}: {net_flow:+} {flow_icon} ({segment['arrivals']} in, {segment['throughput']} out)")

        # Health Alerts
        if "alerts" in scorecard and scorecard["alerts"]:
            triggered_alerts = [a for a in scorecard["alerts"] if a["triggered"]]
            if triggered_alerts:
                print(f"\n🚨 HEALTH ALERTS ({len(triggered_alerts)} triggered):")
                for alert in triggered_alerts:
                    print(f"- {alert['title']}: {alert['rationale']}")
                    print(f"  → {alert['remediation']}")

        # Overall Status (fallback for non-statistical mode)
        if "alerts" not in scorecard:
            flow_status = current_week.get("flow_status", "UNKNOWN")
            status_icon = NetFlowCalculationCommand._get_status_icon(flow_status)
            print(f"\nSTATUS: {status_icon} {flow_status}")

        # Verbose details
        if args.verbose:
            details = scorecard.get("details", {})
            arrival_issues = details.get("arrival_issues", [])
            completed_issues = details.get("completed_issues", [])

            if arrival_issues:
                print(f"\n📥 ARRIVAL ISSUES ({len(arrival_issues)}):")
                for issue in arrival_issues[:10]:
                    print(f"  {issue.get('key')}: {issue.get('summary', '')[:60]}")
                if len(arrival_issues) > 10:
                    print(f"  ... and {len(arrival_issues) - 10} more")

            if completed_issues:
                print(f"\n✅ COMPLETED ISSUES ({len(completed_issues)}):")
                for issue in completed_issues[:10]:
                    print(f"  {issue.get('key')}: {issue.get('summary', '')[:60]}")
                if len(completed_issues) > 10:
                    print(f"  ... and {len(completed_issues) - 10} more")

        print("\n" + "=" * len(header))

    @staticmethod
    def _get_net_flow_status(net_flow: int) -> tuple[str, str]:
        """Get icon and text for the net flow value."""
        if net_flow > 5:
            return "🚨", "Backlog growing critically"
        if net_flow > 0:
            return "⚠️", "Backlog growing"
        if net_flow == 0:
            return "✅", "Balanced"
        if net_flow < 0:
            return "✅", "Backlog shrinking"
        return "", ""

    @staticmethod
    def _get_status_icon(flow_status: str) -> str:
        """Get icon for the overall flow status."""
        status_icons = {
            "CRITICAL_BOTTLENECK": "🚨 ",
            "MINOR_BOTTLENECK": "⚠️ ",
            "HEALTHY_FLOW": "✅ ",
            "BALANCED": "⚖️ ",
        }
        return status_icons.get(flow_status, "❓")
