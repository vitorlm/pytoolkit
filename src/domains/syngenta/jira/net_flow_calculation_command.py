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

            # Generate the scorecard
            scorecard = service.generate_net_flow_scorecard(
                project_key=args.project_key,
                end_date=args.end_date,
                issue_types=issue_types,
                team=args.team,
                include_subtasks=args.include_subtasks,
                output_format=args.output_format,
                verbose=args.verbose,
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
                print("\n❌ Erro na API do Jira (400 - Bad Request):")
                print("   A requisição para a API do Jira foi malformada. Verifique a JQL e os campos solicitados.")
                print(f"\n   Detalhes do Erro: {error_str}")
            else:
                print(f"\n❌ Ocorreu um erro inesperado: {e}")
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
            f"🌊 NET FLOW Health - Semana {week_number} ({start_date.strftime('%b %d')}-{end_date.strftime('%b %d')})"
        )
        print("\n" + "=" * len(header))
        print(header)
        print(f"(Análise ancorada em: {anchor_date})")
        print("=" * len(header))

        # NET FLOW ANALYSIS
        print("\nNET FLOW ANALYSIS:")
        net_flow = current_week.get("net_flow", 0)
        flow_ratio = current_week.get("flow_ratio", 0)
        status_icon, status_text = NetFlowCalculationCommand._get_net_flow_status(net_flow)
        print(f"- Net Flow:     {net_flow:+} {status_icon} ({status_text})")
        print(f"- Flow Ratio:   {flow_ratio:.0f}% ⚠️ (target: >85%)")
        print(f"- Arrival:      {current_week.get('arrival_rate', 0)} issues")
        print(f"- Throughput:   {current_week.get('throughput', 0)} issues")

        # FLOW PATTERNS
        print("\nFLOW PATTERNS:")
        arrival_volatility = current_week.get("arrival_volatility", 0)
        print(f"- Arrival Volatility: {arrival_volatility:.0f}%")

        # ROLLING 4 WEEKS TREND
        if trend:
            print("\nROLLING 4 WEEKS TREND:")
            trend_indicator = ""
            if len(trend) > 1:
                previous_net_flow = trend[-2]["net_flow"]
                if net_flow < previous_net_flow:
                    trend_indicator = "✅ (improving)"
                elif net_flow > previous_net_flow:
                    trend_indicator = "🚨 (worsening)"
                else:
                    trend_indicator = "(stable)"

            for i, week_data in enumerate(trend):
                week_num = week_data["week_number"]
                nf = week_data["net_flow"]
                icon, _ = NetFlowCalculationCommand._get_net_flow_status(nf)
                is_last_week = i == len(trend) - 1
                indicator = trend_indicator if is_last_week else ""
                print(f"- Sem {week_num}: Net Flow {nf:+} {icon} {indicator}")
                print(f"  (Arrival: {week_data['arrival_rate']}, Throughput: {week_data['throughput']})")

        # FLOW EFFICIENCY
        flow_efficiency = current_week.get("flow_efficiency", 0)
        print(f"\nFLOW EFFICIENCY: {flow_efficiency:.0f}% (target: >40%)")

        # CONSTRAINT ANALYSIS
        bottleneck = current_week.get("primary_bottleneck", "N/A")
        print("\nCONSTRAINT ANALYSIS:")
        print(f"- Primary Bottleneck: {bottleneck}")

        # Overall Status
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
            return "🚨", "Backlog crescendo criticamente"
        if net_flow > 0:
            return "⚠️", "Backlog crescendo"
        if net_flow == 0:
            return "✅", "Balanceado"
        if net_flow < 0:
            return "✅", "Backlog diminuindo"
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
