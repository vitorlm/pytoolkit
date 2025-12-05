from argparse import ArgumentParser, Namespace

from domains.syngenta.jira.wip_age_tracking_service import WipAgeTrackingService
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager


class WipAgeTrackingCommand(BaseCommand):
    @staticmethod
    def get_name() -> str:
        return "wip-age-tracking"

    @staticmethod
    def get_description() -> str:
        return "Track and analyze age of issues in Work In Progress statuses."

    @staticmethod
    def get_help() -> str:
        return (
            "This command analyzes WIP issue aging patterns, calculates percentiles, "
            "and provides alerts for issues that exceed aging thresholds. "
            "Helps identify potential bottlenecks and flow issues.\n\n"
            "Examples:\n"
            "  python src/main.py syngenta jira wip-age-tracking --project-key CWS --alert-threshold 5\n"
            "  python src/main.py syngenta jira wip-age-tracking --project-key CWS --team 'Squad A' --verbose\n"
            "  python src/main.py syngenta jira wip-age-tracking --project-key CWS --issue-types Story,Task --output-file results.json"
        )

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--project-key",
            required=True,
            help="JIRA project key (e.g., 'CWS', 'PROJ')",
        )
        parser.add_argument(
            "--alert-threshold",
            type=int,
            default=5,
            help="Days threshold for aging alerts (default: 5)",
        )
        parser.add_argument("--team", help="Filter by team using Squad[Dropdown] field")
        parser.add_argument(
            "--issue-types",
            default="Story,Task,Bug",
            help="Comma-separated issue types (default: 'Story,Task,Bug')",
        )
        parser.add_argument("--output-file", help="Save results to specific JSON file")
        parser.add_argument("--verbose", action="store_true", help="Enable detailed issue-level output")
        parser.add_argument(
            "--include-subtasks",
            action="store_true",
            help="Include subtasks in analysis",
        )
        parser.add_argument(
            "--output-format",
            type=str,
            required=False,
            help="Optional output format: 'json' or 'md' (default: console only).",
            choices=["json", "md"],
            default="console",
        )

    @staticmethod
    def main(args: Namespace):
        # CRITICAL: ALWAYS start with this
        ensure_env_loaded()

        logger = LogManager.get_instance().get_logger("WipAgeTrackingCommand")

        try:
            # Parse issue types
            issue_types = [t.strip() for t in args.issue_types.split(",")] if args.issue_types else None

            # Initialize service
            service = WipAgeTrackingService()

            # Execute analysis
            results = service.calculate_wip_age(
                project_key=args.project_key,
                alert_threshold=args.alert_threshold,
                team=args.team,
                issue_types=issue_types or [],
                include_subtasks=args.include_subtasks,
                output_format=args.output_format,
                output_file=args.output_file,
                verbose=args.verbose,
            )

            # Display summary
            if results:
                logger.info("WIP Age Analysis completed successfully")
                WipAgeTrackingCommand._print_wip_report(results, args)
                if args.output_format in ["json", "md"]:
                    print(f"\nDetailed report saved in {args.output_format.upper()} format")
            else:
                logger.error("WIP Age Analysis failed")
                exit(1)

            print("WIP age tracking completed successfully")

        except Exception as e:
            logger.error(f"WIP age tracking failed: {e}")
            exit(1)

    @staticmethod
    def _print_wip_report(results: dict, args: Namespace):
        """Print the WIP age tracking report to the console in a formatted style."""
        metadata = results.get("metadata", {})
        summary = results.get("summary", {})
        statistics = results.get("statistics", {}).get("age_distribution", {})
        status_breakdown = results.get("status_breakdown", {})
        alerts = results.get("alerts", {})
        insights = results.get("insights", [])

        project_key = metadata.get("project_key", "N/A")
        alert_threshold = metadata.get("alert_threshold_days", 5)
        team = metadata.get("team", "All Teams")
        analysis_date = metadata.get("analysis_date", "")

        # Parse analysis date for display
        try:
            from datetime import datetime

            date_obj = datetime.fromisoformat(analysis_date.replace("Z", "+00:00"))
            formatted_date = date_obj.strftime("%Y-%m-%d")
        except Exception:
            formatted_date = analysis_date

        header = f"⏰ WIP AGE Health - {project_key} (Threshold: {alert_threshold} days)"
        print("\n" + "=" * len(header))
        print(header)
        print(f"(Análise executada em: {formatted_date} | Team: {team})")
        print("=" * len(header))

        # WIP AGE OVERVIEW
        print("\nWIP AGE OVERVIEW:")
        total_wip = summary.get("total_wip_issues", 0)
        issues_over_threshold = summary.get("issues_over_threshold", 0)
        avg_age = summary.get("average_age_days", 0)
        median_age = summary.get("median_age_days", 0)
        oldest_age = summary.get("oldest_issue_age_days", 0)

        status_icon, status_text = WipAgeTrackingCommand._get_wip_status(issues_over_threshold, total_wip)
        print(f"- Total WIP Issues: {total_wip} 📊")
        print(f"- Over Threshold:   {issues_over_threshold} {status_icon} ({status_text})")
        print(f"- Average Age:      {avg_age:.1f} days {WipAgeTrackingCommand._get_age_icon(avg_age, alert_threshold)}")
        print(
            f"- Median Age:       {median_age:.1f} days {WipAgeTrackingCommand._get_age_icon(median_age, alert_threshold)}"
        )
        print(
            f"- Oldest Issue:     {oldest_age} days {WipAgeTrackingCommand._get_age_icon(oldest_age, alert_threshold * 2)}"
        )

        # AGE DISTRIBUTION
        if statistics:
            print("\nAGE DISTRIBUTION:")
            percentiles = statistics.get("percentiles", {})
            print(f"- P50 (Median):     {percentiles.get('p50', 0):.1f} days")
            print(f"- P75:              {percentiles.get('p75', 0):.1f} days")
            print(
                f"- P90:              {percentiles.get('p90', 0):.1f} days {WipAgeTrackingCommand._get_percentile_icon(percentiles.get('p90', 0), alert_threshold)}"
            )
            print(
                f"- P95:              {percentiles.get('p95', 0):.1f} days {WipAgeTrackingCommand._get_percentile_icon(percentiles.get('p95', 0), alert_threshold)}"
            )

        # STATUS BREAKDOWN
        if status_breakdown:
            print("\nSTATUS BREAKDOWN:")
            for status, data in status_breakdown.items():
                issue_count = data.get("issue_count", 0)
                avg_status_age = data.get("avg_age_days", 0)
                status_over_threshold = data.get("issues_over_threshold", 0)
                status_health = WipAgeTrackingCommand._get_status_health_icon(status_over_threshold, issue_count)
                print(f"- {status}: {issue_count} issues (avg: {avg_status_age:.1f}d) {status_health}")
                if status_over_threshold > 0:
                    print(f"  └─ {status_over_threshold} over threshold ⚠️")

        # AGING ALERTS
        alert_issues = alerts.get("issues_over_threshold", [])
        if alert_issues:
            print(f"\n🚨 AGING ALERTS ({len(alert_issues)} issues):")
            for issue in alert_issues[:8]:  # Show first 8 alerts
                key = issue.get("key", "N/A")
                age = issue.get("age_days", 0)
                status = issue.get("status", "N/A")
                assignee = issue.get("assignee") or "Unassigned"
                print(f"- {key}: {age}d in {status} ({assignee})")

            if len(alert_issues) > 8:
                print(f"  ... and {len(alert_issues) - 8} more issues")

        # FLOW HEALTH ASSESSMENT
        risk_level = WipAgeTrackingCommand._get_risk_level(issues_over_threshold, total_wip)
        risk_icon = WipAgeTrackingCommand._get_risk_icon(risk_level)
        print("\nFLOW HEALTH ASSESSMENT:")
        print(f"- Risk Level:       {risk_level} {risk_icon}")
        print(f"- Threshold Rate:   {(issues_over_threshold / total_wip * 100) if total_wip > 0 else 0:.1f}%")

        # KEY INSIGHTS
        if insights:
            print("\nKEY INSIGHTS:")
            for insight in insights:
                print(f"• {insight}")

        # Verbose details
        if args.verbose:
            detailed_issues = results.get("detailed_issues", [])
            if detailed_issues:
                print(f"\n📋 ALL WIP ISSUES ({len(detailed_issues)}):")
                for issue in detailed_issues[:12]:
                    key = issue.get("key", "N/A")
                    age = issue.get("age_days", 0)
                    status = issue.get("status", "N/A")
                    team_name = issue.get("team", "No Team")
                    print(f"  {key}: {age}d in {status} ({team_name})")
                if len(detailed_issues) > 12:
                    print(f"  ... and {len(detailed_issues) - 12} more")

        print("\n" + "=" * len(header))

    @staticmethod
    def _get_wip_status(issues_over_threshold: int, total_issues: int) -> tuple[str, str]:
        """Get icon and text for WIP status."""
        if total_issues == 0:
            return "ℹ️", "No WIP issues"

        percentage = (issues_over_threshold / total_issues) * 100
        if percentage == 0:
            return "✅", "All within threshold"
        elif percentage <= 20:
            return "⚠️", "Few issues aging"
        elif percentage <= 40:
            return "🚨", "Multiple issues aging"
        else:
            return "🚨", "Critical aging detected"

    @staticmethod
    def _get_age_icon(age: float, threshold: float) -> str:
        """Get icon for age assessment."""
        if age <= threshold * 0.7:
            return "✅"
        elif age <= threshold:
            return "⚠️"
        else:
            return "🚨"

    @staticmethod
    def _get_percentile_icon(percentile_value: float, threshold: int) -> str:
        """Get icon for percentile assessment."""
        if percentile_value <= threshold:
            return "✅"
        elif percentile_value <= threshold * 1.5:
            return "⚠️"
        else:
            return "🚨"

    @staticmethod
    def _get_status_health_icon(over_threshold: int, total: int) -> str:
        """Get health icon for status."""
        if total == 0:
            return "➖"

        percentage = (over_threshold / total) * 100
        if percentage == 0:
            return "✅"
        elif percentage <= 25:
            return "⚠️"
        else:
            return "🚨"

    @staticmethod
    def _get_risk_level(issues_over_threshold: int, total_issues: int) -> str:
        """Get risk level assessment."""
        if total_issues == 0:
            return "No Issues"

        percentage = (issues_over_threshold / total_issues) * 100

        if percentage == 0:
            return "LOW RISK"
        elif percentage <= 15:
            return "MODERATE RISK"
        elif percentage <= 30:
            return "HIGH RISK"
        else:
            return "CRITICAL RISK"

    @staticmethod
    def _get_risk_icon(risk_level: str) -> str:
        """Get icon for risk level."""
        risk_icons = {
            "NO ISSUES": "ℹ️",
            "LOW RISK": "✅",
            "MODERATE RISK": "⚠️",
            "HIGH RISK": "🚨",
            "CRITICAL RISK": "🚨",
        }
        return risk_icons.get(risk_level, "❓")
