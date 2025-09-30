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

import os
from argparse import ArgumentParser, Namespace
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from domains.syngenta.jira.net_flow_calculation_service import NetFlowCalculationService
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager
from utils.output_manager import OutputManager
from utils.summary_helpers import _has_value, _isoz


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
            "--summary-output",
            type=str,
            choices=["auto", "json", "none"],
            default="auto",
            help=(
                "Control summary metrics persistence: 'auto' stores it alongside generated reports, "
                "'json' forces a JSON summary via OutputManager even without a report, and 'none' skips it."
            ),
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

                try:
                    summary_mode = getattr(args, "summary_output", "auto")
                    raw_output_path = scorecard.get("output_file")
                    summary_path = NetFlowCalculationCommand._emit_summary(
                        scorecard,
                        summary_mode,
                        raw_output_path,
                        args,
                    )
                    if summary_path:
                        print(f"[summary] wrote: {summary_path}")
                except Exception as summary_error:
                    logger.warning(f"Failed to write summary metrics: {summary_error}")
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

    @staticmethod
    def _emit_summary(
        scorecard: Dict[str, Any],
        summary_mode: str,
        existing_output_path: Optional[str],
        args: Namespace,
    ) -> Optional[str]:
        if summary_mode == "none":
            return None

        raw_data_path = os.path.abspath(existing_output_path) if existing_output_path else None
        metrics_payload = NetFlowCalculationCommand._build_summary_metrics(scorecard, raw_data_path)
        if not metrics_payload:
            return None

        sub_dir, base_name = NetFlowCalculationCommand._summary_output_defaults(args, scorecard)
        summary_path: Optional[str] = None

        if existing_output_path:
            target_path = NetFlowCalculationCommand._summary_path_for_existing(existing_output_path)
            summary_path = OutputManager.save_summary_report(
                metrics_payload,
                sub_dir,
                base_name,
                output_path=target_path,
            )
            summary_path = os.path.abspath(summary_path)

        if summary_mode == "auto":
            return summary_path

        if summary_path and summary_mode == "json":
            return summary_path

        if summary_mode == "json":
            summary_path = OutputManager.save_summary_report(
                metrics_payload,
                sub_dir,
                base_name,
            )
            return os.path.abspath(summary_path)

        return None

    @staticmethod
    def _build_summary_metrics(scorecard: Dict[str, Any], raw_output_path: Optional[str]) -> List[Dict[str, Any]]:
        metadata = scorecard.get("metadata") or {}
        period_start = _isoz(metadata.get("start_date"))
        period_end = _isoz(metadata.get("end_date"))
        if not period_start or not period_end:
            return []

        period = {"start_date": period_start, "end_date": period_end}
        base_dimensions = NetFlowCalculationCommand._base_dimensions(metadata)
        summary_metrics: List[Dict[str, Any]] = []
        command_name = NetFlowCalculationCommand.get_name()

        current_week = scorecard.get("current_week") or {}
        NetFlowCalculationCommand._append_metric(
            summary_metrics,
            "jira.net_flow.net_flow",
            current_week.get("net_flow"),
            "issues",
            period,
            base_dimensions,
            command_name,
            raw_output_path,
        )
        NetFlowCalculationCommand._append_metric(
            summary_metrics,
            "jira.net_flow.flow_ratio",
            current_week.get("flow_ratio"),
            "percent",
            period,
            base_dimensions,
            command_name,
            raw_output_path,
        )
        NetFlowCalculationCommand._append_metric(
            summary_metrics,
            "jira.net_flow.arrival_rate",
            current_week.get("arrival_rate"),
            "issues",
            period,
            base_dimensions,
            command_name,
            raw_output_path,
        )
        NetFlowCalculationCommand._append_metric(
            summary_metrics,
            "jira.net_flow.throughput",
            current_week.get("throughput"),
            "issues",
            period,
            base_dimensions,
            command_name,
            raw_output_path,
        )
        NetFlowCalculationCommand._append_metric(
            summary_metrics,
            "jira.net_flow.flow_efficiency",
            current_week.get("flow_efficiency"),
            "percent",
            period,
            base_dimensions,
            command_name,
            raw_output_path,
        )

        segments = scorecard.get("segments")
        if isinstance(segments, list):
            for segment in segments:
                if not isinstance(segment, dict):
                    continue
                segment_name = segment.get("segment_name")
                if not segment_name:
                    continue
                segment_dimensions = {**base_dimensions, "issue_type": segment_name}
                NetFlowCalculationCommand._append_metric(
                    summary_metrics,
                    "jira.net_flow.net_flow",
                    segment.get("net_flow"),
                    "issues",
                    period,
                    segment_dimensions,
                    command_name,
                    raw_output_path,
                )
                NetFlowCalculationCommand._append_metric(
                    summary_metrics,
                    "jira.net_flow.arrival_rate",
                    segment.get("arrivals"),
                    "issues",
                    period,
                    segment_dimensions,
                    command_name,
                    raw_output_path,
                )
                NetFlowCalculationCommand._append_metric(
                    summary_metrics,
                    "jira.net_flow.throughput",
                    segment.get("throughput"),
                    "issues",
                    period,
                    segment_dimensions,
                    command_name,
                    raw_output_path,
                )
                arrivals = segment.get("arrivals")
                throughput = segment.get("throughput")
                if _has_value(arrivals) and _has_value(throughput) and arrivals:
                    try:
                        flow_ratio = float(throughput) / float(arrivals) * 100
                    except (TypeError, ValueError, ZeroDivisionError):
                        flow_ratio = None
                    NetFlowCalculationCommand._append_metric(
                        summary_metrics,
                        "jira.net_flow.flow_ratio",
                        flow_ratio,
                        "percent",
                        period,
                        segment_dimensions,
                        command_name,
                        raw_output_path,
                    )

        return summary_metrics

    @staticmethod
    def _summary_output_defaults(args: Namespace, scorecard: Dict[str, Any]) -> Tuple[str, str]:
        metadata = scorecard.get("metadata") or {}
        project_key = metadata.get("project_key") or getattr(args, "project_key", "unknown")
        date_str = datetime.now().strftime("%Y%m%d")
        sub_dir = f"net-flow_{date_str}"
        base_name = f"net_flow_summary_{project_key}"
        return sub_dir, base_name

    @staticmethod
    def _summary_path_for_existing(existing_output_path: str) -> str:
        output_path = Path(existing_output_path)
        summary_filename = f"{output_path.stem}_summary.json"
        return str(output_path.with_name(summary_filename))

    @staticmethod
    def _append_metric(
        container: List[Dict[str, Any]],
        metric_name: str,
        value: Any,
        unit: str,
        period: Dict[str, str],
        dimensions: Dict[str, Any],
        source_command: str,
        raw_data_path: Optional[str],
    ) -> None:
        if not _has_value(value):
            return

        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            return

        cleaned_dimensions = {k: v for k, v in dimensions.items() if _has_value(v) and str(v).strip()}
        container.append(
            {
                "metric_name": metric_name,
                "value": numeric_value,
                "unit": unit,
                "period": period,
                "dimensions": cleaned_dimensions,
                "source_command": source_command,
                "raw_data_path": raw_data_path,
            }
        )

    @staticmethod
    def _base_dimensions(metadata: Dict[str, Any]) -> Dict[str, Any]:
        dimensions: Dict[str, Any] = {}
        project_key = metadata.get("project_key")
        if project_key:
            dimensions["project"] = project_key

        team_value = NetFlowCalculationCommand._normalize_team_metadata(metadata)
        if team_value:
            dimensions["team"] = team_value
        else:
            dimensions["team"] = "overall"
        return dimensions

    @staticmethod
    def _normalize_team_metadata(metadata: Dict[str, Any]) -> Optional[str]:
        teams = metadata.get("teams")
        if isinstance(teams, list):
            cleaned = [str(team).strip() for team in teams if str(team).strip()]
            if len(cleaned) == 1:
                return cleaned[0]
            if cleaned:
                return ",".join(cleaned)

        team_label = metadata.get("team")
        if isinstance(team_label, str) and team_label.strip():
            return team_label.strip()

        return None
