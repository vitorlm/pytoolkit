"""
JIRA Issue Adherence Analysis Command

This command analyzes issue adherence by fetching issues that were resolved within an anchored
window and evaluating their completion status against due dates.

FUNCTIONALITY:
- Fetch issues resolved within an anchor-based window (e.g., last 7/14/30 days)
- Filter issues that have due dates set
- Calculate adherence metrics (on-time, late, overdue)
- Generate detailed adherence reports (Markdown/JSON)

USAGE EXAMPLES (Anchor + Window):

1. Analyze last 7 days anchored on a date:
   python src/main.py syngenta jira issue-adherence --project-key "CWS" \
   --end-date "2025-09-21" --window-days 7

2. Analyze last 14 days with specific issue types:
   python src/main.py syngenta jira issue-adherence --project-key "CWS" \
   --end-date "2025-09-21" --window-days 14 --issue-types "Story,Task,Bug"

3. Analyze last 30 days and export Markdown:
   python src/main.py syngenta jira issue-adherence --project-key "CWS" \
   --end-date "2025-09-21" --window-days 30 --output-format md

4. Analyze with team filter:
   python src/main.py syngenta jira issue-adherence --project-key "CWS" \
   --end-date "2025-09-21" --window-days 7 --team "Catalog"

4b. Analyze with multiple teams (tribe):
   python src/main.py syngenta jira issue-adherence --project-key "CWS" \
   --end-date "2025-09-21" --window-days 7 --teams "Catalog,Platform"

ADHERENCE METRICS:
- On-time: Issues completed on or before due date
- Late: Issues completed after due date
- Overdue: Issues with due date passed but not completed
- No due date: Issues without due date set
"""

import os
from argparse import ArgumentParser, Namespace
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from domains.syngenta.jira.issue_adherence_service import IssueAdherenceService
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager
from utils.output_manager import OutputManager
from utils.summary_helpers import _extract_metric_value, _has_value, _isoz


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
        # Time window (anchor + window)
        parser.add_argument(
            "--end-date",
            type=str,
            required=False,
            help=(
                "Anchor date in YYYY-MM-DD. If provided with --window-days, defines the analysis window. Defaults to today."
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
        parser.add_argument(
            "--summary-output",
            type=str,
            choices=["auto", "json", "none"],
            default="auto",
            help=(
                "Control summary metrics persistence: 'auto' stores alongside generated reports, "
                "'json' forces a summary file even without a report, and 'none' skips it."
            ),
        )
        parser.add_argument(
            "--weighted-adherence",
            action="store_true",
            help="Enable weighted adherence calculation (adds a second adherence metric)",
        )
        parser.add_argument(
            "--early-tolerance-days",
            type=int,
            default=1,
            help="Tolerance (days) for early completion (no tolerance is applied to lateness)",
        )
        parser.add_argument(
            "--early-weight",
            type=float,
            default=1.0,
            help="Penalty weight per day for early completion beyond tolerance",
        )
        parser.add_argument(
            "--late-weight",
            type=float,
            default=3.0,
            help="Penalty weight per day for late completion (no tolerance)",
        )
        parser.add_argument(
            "--no-due-penalty",
            type=float,
            default=15.0,
            help="Fixed penalty applied to completed issues without a due date (when included)",
        )
        parser.add_argument(
            "--extended",
            action="store_true",
            help="Enable extended analysis sections (statistical insights, alerts).",
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
                window_days = int(getattr(args, "window_days", 7))
                start = anchor - timedelta(days=max(window_days - 1, 0))
                end = anchor
                computed_time_period = f"{start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}"

            # Initialize service
            service = IssueAdherenceService()

            # Run adherence analysis
            # Parse teams
            raw_teams = getattr(args, "teams", []) or []
            teams = [t.strip() for entry in raw_teams for t in entry.split(",") if t.strip()] or None

            result = service.analyze_issue_adherence(
                project_key=args.project_key,
                time_period=(computed_time_period or f"{(date.today()-timedelta(days=6)).strftime('%Y-%m-%d')} to {date.today().strftime('%Y-%m-%d')}") ,
                issue_types=issue_types,
                teams=teams,
                status=status,
                include_no_due_date=args.include_no_due_date,
                verbose=args.verbose,
                output_file=args.output_file,
                output_format=args.output_format,
                weighted_adherence=args.weighted_adherence,
                enable_extended=getattr(args, 'extended', False),
                early_tolerance_days=args.early_tolerance_days,
                early_weight=args.early_weight,
                late_weight=args.late_weight,
                no_due_penalty=args.no_due_penalty,
            )

            if result:
                logger.info("Issue adherence analysis completed successfully")

                # Always print a concise summary first
                IssueAdherenceCommand._print_basic_summary(result, args)

                # Handle different output formats
                if args.output_format in ["json", "md"]:
                    output_path = result.get("output_file")
                    if output_path:
                        print("✅ Detailed report saved")
                    else:
                        print("⚠️  Analysis completed but no output file was generated.")

                try:
                    summary_mode = getattr(args, "summary_output", "auto")
                    raw_output_path = result.get("output_file")
                    summary_path = IssueAdherenceCommand._emit_summary(
                        result,
                        summary_mode,
                        raw_output_path,
                        args,
                    )
                    if summary_path:
                        print(f"[summary] wrote: {summary_path}")
                except Exception as summary_error:
                    logger.warning(f"Failed to write summary metrics: {summary_error}")

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

        # Header (standardized)
        metadata = result.get("analysis_metadata", {}) if isinstance(result, dict) else {}
        start = (metadata.get("start_date", "") or "")[:10]
        end = (metadata.get("end_date", "") or "")[:10]
        header = f"📅 ISSUE ADHERENCE - {args.project_key} ({start} to {end})"
        print("\n" + "=" * len(header))
        print(header)
        if end:
            print(f"(Anchored on: {end})")
        print("=" * len(header))

        # Project Information (concise)
        # Teams filter (supports --team/--teams; prints flattened unique list)
        teams_list = None
        raw_teams = getattr(args, "teams", None)
        if raw_teams:
            seen = set()
            flattened = []
            for entry in raw_teams:
                for t in str(entry).split(","):
                    t = t.strip()
                    if t and t not in seen:
                        seen.add(t)
                        flattened.append(t)
            teams_list = flattened if flattened else None
        if teams_list:
            print(f"Teams: {', '.join(teams_list)}")
        print(f"Issue Types: {', '.join(issue_types)}")
        # Fallback to metadata if no CLI-provided teams
        if not teams_list and isinstance(result, dict):
            meta = result.get("analysis_metadata", {}) or {}
            meta_team = meta.get("team")
            meta_teams = meta.get("teams") if isinstance(meta.get("teams"), list) else None
            if meta_team:
                print(f"Teams: {meta_team}")
            elif meta_teams:
                print(f"Teams: {', '.join(meta_teams)}")

        # Executive Summary
        print("📈 EXECUTIVE SUMMARY")
        print(f"   Total Issues Analyzed: {total_issues}")
        print(f"   Issues with Due Dates: {issues_with_due_dates}")
        print(f"   Risk Assessment: {risk_assessment}")
        print()
        # Output file path (if saved)
        output_path = result.get("output_file") if isinstance(result, dict) else None
        if output_path:
            print("Output file:")
            print(f"- {output_path}")
            print()

        # Adherence metrics (separated)
        print("📐 ADHERENCE METRICS")
        print(f"   📊 Adherence:   {adherence_rate:.1f}%")
        weighted = result.get("weighted_metrics")
        if weighted:
            wa = weighted.get("weighted_adherence", 0)
            params = weighted.get("parameters", {})
            print(f"   ⚖️ Weighted Adherence: {wa:.1f}%")
            print("   Parameters used:")
            print(f"     • Early tolerance (days): {params.get('early_tolerance_days')}")
            print(f"     • Early weight (pt/day): {params.get('early_weight')}")
            print(f"     • Late weight (pt/day):  {params.get('late_weight')}")
            print(f"     • No-due penalty (pts):  {params.get('no_due_penalty')}")
            print(
                "   Note: Weighted adherence penalizes lateness more, allows small early tolerance, and applies a fixed penalty for completed issues without due date."
            )
        print()

        # Adherence Breakdown
        print("📋 ADHERENCE BREAKDOWN")

        status_data = [
            ("early", "Early Completion", "🟣"),
            ("on_time", "On-time Completion", "✅"),
            ("late", "Late Completion", "🟡"),
            ("in_progress", "In Progress", "🔵"),
        ]

        if args.include_no_due_date:
            status_data.append(("no_due_date", "No Due Date", "⚪"))

        # Print status breakdown in a formatted way
        for status_key, status_label, emoji in status_data:
            count = metrics.get(status_key, 0)
            percentage = metrics.get(f"{status_key}_percentage", 0)

            if count > 0 or status_key in ["early", "on_time", "late"]:
                print(f"   {emoji} {status_label:<20} {count:>3} issues ({percentage:>5.1f}%)")

        print()

        # Performance Analysis
        print("🎯 PERFORMANCE ANALYSIS")

        weighted_perf = result.get("weighted_metrics")
        early_count = metrics.get("early", 0)
        on_time_count = metrics.get("on_time", 0)
        late_count = metrics.get("late", 0)
        total_completed = early_count + on_time_count + late_count

        if weighted_perf:
            wa = weighted_perf.get("weighted_adherence", 0.0)
            penalties = weighted_perf.get("penalties") or {}
            avg_pen = penalties.get("avg_per_item_capped", 0.0)
            cap = penalties.get("cap_per_item", 100.0)
            print(f"   ✅ Weighted Completion Score: {wa:.1f}%")
            print(f"      Avg penalty per completed item (capped at {cap:.0f}): {avg_pen:.1f} pts")
            # Penalty breakdown (capped to align with score)
            late_total = penalties.get("late_total_capped", penalties.get("late_total", 0.0))
            early_total = penalties.get("early_total_capped", penalties.get("early_total", 0.0))
            ndd_total = penalties.get("no_due_total_capped", penalties.get("no_due_total", 0.0))
            print("   🧮 Penalty breakdown (capped points):")
            print(f"      • Late: {late_total:.1f} | Early: {early_total:.1f} | No-due: {ndd_total:.1f}")

            # Estimated deviation days and affected items for leadership view
            params = weighted_perf.get("parameters") or {}
            lw = float(params.get("late_weight", 0) or 0)
            ew = float(params.get("early_weight", 0) or 0)
            ndp = float(params.get("no_due_penalty", 0) or 0)
            late_days = (late_total / lw) if lw > 0 else 0.0
            early_days = (early_total / ew) if ew > 0 else 0.0
            ndd_items = (ndd_total / ndp) if ndp > 0 else 0.0
            print("   🗓️ Estimated impact:")
            print(
                f"      • Late days: {late_days:.1f}d | Early days beyond tolerance: {early_days:.1f}d | No-due affected items: {ndd_items:.1f}"
            )

            # Impact on score: what could have been achieved without penalties
            total_items = int(weighted_perf.get("included_items", 0) or 0)
            total_capped = float(penalties.get("total_capped", 0.0) or 0.0)
            potential = 100.0
            achieved = wa
            total_impact = potential - achieved  # should equal avg_capped
            print("   🎯 Score impact (vs. no penalties):")
            print(
                f"      • Potential score: {potential:.1f}% → Achieved: {achieved:.1f}% (impact: -{total_impact:.1f} pts)"
            )

            if total_items > 0 and total_impact > 0:
                late_pts = late_total / total_items
                early_pts = early_total / total_items
                ndd_pts = ndd_total / total_items
                # Shares
                late_share = (late_total / total_capped * 100.0) if total_capped > 0 else 0.0
                early_share = (early_total / total_capped * 100.0) if total_capped > 0 else 0.0
                ndd_share = (ndd_total / total_capped * 100.0) if total_capped > 0 else 0.0
                print("      • Impact by driver (avg pts per item):")
                print(f"         - Late: -{late_pts:.1f} | Early: -{early_pts:.1f} | No-due: -{ndd_pts:.1f}")
                print("      • Share of total penalty:")
                print(f"         - Late: {late_share:.1f}% | Early: {early_share:.1f}% | No-due: {ndd_share:.1f}%")
            if late_count > 0:
                print(f"   📋 Improvement Area: {late_count} issues completed late")
        else:
            successful_completion = early_count + on_time_count
            if total_completed > 0:
                success_rate = (successful_completion / total_completed) * 100
                print(f"   ✅ Completion Success Rate: {success_rate:.1f}%")
                print(f"      Successfully completed on or before due date: {successful_completion}/{total_completed}")
            if late_count > 0:
                print(f"   📋 Improvement Area: {late_count} issues completed late")

        print()

        # Statistical Insights
        statistical_insights = result.get("statistical_insights")
        if statistical_insights and statistical_insights.get("total_completed_with_due_dates", 0) > 0:
            print("📊 STATISTICAL INSIGHTS")
            delivery_stats = statistical_insights.get("delivery_time_stats", {})
            percentiles = statistical_insights.get("percentile_analysis", {})
            outliers = statistical_insights.get("outlier_analysis", {})

            print("   📈 Delivery Time Statistics (days from due date):")
            print(
                f"      • Mean: {delivery_stats.get('mean', 0):.1f} | Median: {delivery_stats.get('median', 0):.1f} | Std Dev: {delivery_stats.get('std_dev', 0):.1f}"
            )
            print(f"      • Range: {delivery_stats.get('min', 0)} to {delivery_stats.get('max', 0)} days")

            print("   📊 Percentile Analysis:")
            print(
                f"      • P25: {percentiles.get('p25', 0):.1f} | P50: {percentiles.get('p50', 0):.1f} | P75: {percentiles.get('p75', 0):.1f} | P90: {percentiles.get('p90', 0):.1f}"
            )

            if outliers.get("outlier_count", 0) > 0:
                print("   🚨 Outlier Analysis:")
                print(
                    f"      • {outliers.get('outlier_count', 0)} outliers ({outliers.get('outlier_percentage', 0):.1f}% of completed issues)"
                )
                extreme_late = outliers.get("extreme_late", [])
                extreme_early = outliers.get("extreme_early", [])
                if extreme_late:
                    print(
                        f"      • Extremely late: {len(extreme_late)} issues (worst: {max(extreme_late) if extreme_late else 0} days)"
                    )
                if extreme_early:
                    print(
                        f"      • Extremely early: {len(extreme_early)} issues (earliest: {min(extreme_early) if extreme_early else 0} days)"
                    )
            print()

        # Segmentation Analysis
        segmentation = result.get("segmentation_analysis")
        if segmentation:
            print("👥 SEGMENTATION ANALYSIS")

            # Show worst performing teams/types
            by_team = segmentation.get("by_team", {})
            if len(by_team) > 1:  # Only show if there are multiple teams
                print("   📋 By Team (worst performing first):")
                team_items = list(by_team.items())[:3]  # Show top 3 worst
                for team, metrics in team_items:
                    adherence = metrics.get("adherence_rate", 0)
                    total = metrics.get("total_completed", 0)
                    if total > 0:
                        risk_emoji = "🔴" if adherence < 60 else "🟡" if adherence < 80 else "🟢"
                        print(f"      {risk_emoji} {team}: {adherence:.1f}% ({total} completed issues)")
                print()

            by_type = segmentation.get("by_issue_type", {})
            if len(by_type) > 1:  # Only show if there are multiple issue types
                print("   🏷️  By Issue Type (worst performing first):")
                type_items = list(by_type.items())[:3]  # Show top 3 worst
                for issue_type, metrics in type_items:
                    adherence = metrics.get("adherence_rate", 0)
                    total = metrics.get("total_completed", 0)
                    if total > 0:
                        risk_emoji = "🔴" if adherence < 60 else "🟡" if adherence < 80 else "🟢"
                        print(f"      {risk_emoji} {issue_type}: {adherence:.1f}% ({total} completed issues)")
                print()

        # Due Date Coverage Analysis
        due_date_coverage = result.get("due_date_coverage")
        if due_date_coverage:
            overall = due_date_coverage.get("overall", {})
            coverage_pct = overall.get("coverage_percentage", 0)
            print("📅 DUE DATE COVERAGE ANALYSIS")
            print(f"   📊 Overall Coverage: {coverage_pct:.1f}%")
            print(f"      • Issues with due dates: {overall.get('issues_with_due_dates', 0)}")
            print(f"      • Issues without due dates: {overall.get('issues_without_due_dates', 0)}")

            if coverage_pct < 80:
                print("   ⚠️  Coverage below 80%")
                # Show teams with worst coverage
                by_team_coverage = due_date_coverage.get("by_team", {})
                worst_teams = [
                    (team, metrics)
                    for team, metrics in by_team_coverage.items()
                    if metrics.get("coverage_rate", 0) < 80
                ][:2]
                if worst_teams:
                    print("      Teams needing improvement:")
                    for team, metrics in worst_teams:
                        rate = metrics.get("coverage_rate", 0)
                        print(f"         • {team}: {rate:.1f}% coverage")
            print()

        # Time Distribution of deviations from due date (completed items)
        time_dist = result.get("time_distribution")
        if time_dist:
            print("📊 TIME DISTRIBUTION")
            print("-" * 40)
            # Single fixed-width table, as requested
            bins = time_dist.get("bins", [])
            max_count = max((b.get("count", 0) for b in bins), default=0)
            # Header and ruler as per the pattern
            print(" Bucket    | Issues |  Percent | Bar")
            print(" --------- | ------ | -------- | --------------------")
            for b in bins:
                label = b.get("label", "")
                count = int(b.get("count", 0))
                pct = float(b.get("percentage", 0.0))
                # Bar scaled to 20 chars
                if max_count > 0 and count > 0:
                    bar_len = max(1, int(round((count / max_count) * 20)))
                else:
                    bar_len = 0
                bar = "█" * bar_len
                # Format fixed-width columns
                percent_text = f"{pct:.1f}%".rjust(8)
                print(f" {label:<9} | {count:>6} | {percent_text} | {bar}")
            total_considered = time_dist.get("total_considered", 0)
            # Total line
            total_percent = "100.0%".rjust(8)
            print(f" Total     | {total_considered:>6} | {total_percent} |")
            print()

        # Recommendations (prefer weighted if available)
        print("💡 RECOMMENDATIONS")
        weighted_perf = result.get("weighted_metrics")
        if weighted_perf:
            wa = weighted_perf.get("weighted_adherence", 0.0)
            penalties = weighted_perf.get("penalties") or {}
            late_total = penalties.get("late_total", 0.0)
            early_total = penalties.get("early_total", 0.0)
            ndd_total = penalties.get("no_due_total", 0.0)

            # Tailored suggestions based on dominant penalty drivers
            if late_total >= early_total and late_total >= ndd_total:
                print("   ⚠️ Focus: Reduce lateness penalties")
                print("      • Strengthen due-date governance and commitments")
                print("      • Add buffer for high-variability work; limit WIP")
                print("      • Use risk flags/alerts before due date (SLA alerts)")
            if ndd_total > 0:
                print("   ⚠️ Focus: Enforce due-date completeness")
                print("      • Make due date mandatory for planned work")
                print("      • Add checks in intake/refinement workflows")
            if early_total > late_total and early_total > 0:
                print("   ⚠️ Focus: Reduce early delivery variance")
                print("      • Improve estimation windows and milestone alignment")
                print("      • Synchronize dependencies to avoid too-early handoffs")

            # Overall stance based on weighted score
            if wa < 60:
                print("   🚨 IMMEDIATE ACTIONS REQUIRED:")
                print("      • Weekly review of missed/at-risk deadlines")
                print("      • Rebalance capacity and renegotiate targets where needed")
            elif wa < 80:
                print("   ⚠️  IMPROVEMENT OPPORTUNITIES:")
                print("      • Enhance due date visibility and tracking")
                print("      • Implement early warning systems for at-risk issues")
                print("      • Review resource allocation and capacity planning")
            else:
                print("   ✅ MAINTAIN EXCELLENCE:")
                print("      • Continue current practices and monitoring")
                print("      • Share best practices with other teams")
                print("      • Focus on continuous improvement")
        else:
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
        print("   • Analysis considers issues resolved within the specified time period")
        print("   • Legacy adherence = (Early + On-time) / Completed issues")
        print(
            "   • Weighted adherence averages per-issue scores with asymmetric penalties: late > early; no tolerance for late; early uses tolerance-days; completed issues without due date may receive a fixed penalty when included"
        )

        # Explicit list of issues without due date at the end (when included in analysis)
        if args.include_no_due_date:
            issues_list = result.get("issues", [])
            no_due_items = [i for i in issues_list if i.get("adherence_status") == "no_due_date"]
            if no_due_items:
                print()
                print("📎 ISSUES WITHOUT DUE DATE")
                for item in no_due_items:
                    team_name = item.get("team") or "Not set"
                    print(f"   • {item.get('issue_key', 'N/A')} — Team: {team_name}")

        if args.output_file:
            print(f"\n📄 Detailed report saved to: {args.output_file}")

        print("=" * 79)

    @staticmethod
    def _emit_summary(
        result: Dict[str, Any],
        summary_mode: str,
        existing_output_path: Optional[str],
        args: Namespace,
    ) -> Optional[str]:
        if summary_mode == "none":
            return None

        raw_data_path = os.path.abspath(existing_output_path) if existing_output_path else None
        metrics_payload = IssueAdherenceCommand._build_summary_metrics(result, raw_data_path)
        if not metrics_payload:
            return None

        sub_dir, base_name = IssueAdherenceCommand._summary_output_defaults(args, result)
        summary_path: Optional[str] = None

        if existing_output_path:
            target_path = IssueAdherenceCommand._summary_path_for_existing(existing_output_path)
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
    def _build_summary_metrics(result: Dict[str, Any], raw_output_path: Optional[str]) -> List[Dict[str, Any]]:
        metadata = result.get("analysis_metadata") or {}
        period_start = _isoz(metadata.get("start_date"))
        period_end = _isoz(metadata.get("end_date"))
        if not period_start or not period_end:
            return []

        period = {"start_date": period_start, "end_date": period_end}
        base_dimensions = IssueAdherenceCommand._base_dimensions(metadata)
        summary_metrics: List[Dict[str, Any]] = []
        command_name = IssueAdherenceCommand.get_name()

        metrics_block = result.get("metrics") or {}
        IssueAdherenceCommand._append_metric(
            summary_metrics,
            "jira.issue_adherence.adherence_rate",
            metrics_block.get("adherence_rate"),
            "percent",
            period,
            base_dimensions,
            command_name,
            raw_output_path,
        )
        weighted_block = result.get("weighted_metrics") or {}
        IssueAdherenceCommand._append_metric(
            summary_metrics,
            "jira.issue_adherence.weighted_adherence",
            weighted_block.get("weighted_adherence"),
            "percent",
            period,
            base_dimensions,
            command_name,
            raw_output_path,
        )
        coverage_value = _extract_metric_value(result, ("due_date_coverage", "overall", "coverage_percentage"))
        IssueAdherenceCommand._append_metric(
            summary_metrics,
            "jira.issue_adherence.due_date_coverage_percent",
            coverage_value,
            "percent",
            period,
            base_dimensions,
            command_name,
            raw_output_path,
        )
        IssueAdherenceCommand._append_metric(
            summary_metrics,
            "jira.issue_adherence.on_time_completion_count",
            metrics_block.get("on_time"),
            "issues",
            period,
            base_dimensions,
            command_name,
            raw_output_path,
        )
        IssueAdherenceCommand._append_metric(
            summary_metrics,
            "jira.issue_adherence.late_completion_count",
            metrics_block.get("late"),
            "issues",
            period,
            base_dimensions,
            command_name,
            raw_output_path,
        )

        segmentation = result.get("segmentation_analysis") or {}
        IssueAdherenceCommand._append_segmentation_metrics(
            summary_metrics,
            segmentation.get("by_team"),
            period,
            base_dimensions,
            command_name,
            raw_output_path,
            dimension_key="team",
        )
        IssueAdherenceCommand._append_segmentation_metrics(
            summary_metrics,
            segmentation.get("by_issue_type"),
            period,
            base_dimensions,
            command_name,
            raw_output_path,
            dimension_key="issue_type",
        )

        return summary_metrics

    @staticmethod
    def _summary_output_defaults(args: Namespace, result: Dict[str, Any]) -> Tuple[str, str]:
        metadata = result.get("analysis_metadata") or {}
        project_key = metadata.get("project_key") or getattr(args, "project_key", "unknown")
        date_str = datetime.now().strftime("%Y%m%d")
        sub_dir = f"issue-adherence_{date_str}"
        base_name = f"issue_adherence_summary_{project_key}"
        return sub_dir, base_name

    @staticmethod
    def _summary_path_for_existing(existing_output_path: str) -> str:
        output_path = Path(existing_output_path)
        summary_filename = f"{output_path.stem}_summary.json"
        return str(output_path.with_name(summary_filename))

    @staticmethod
    def _append_segmentation_metrics(
        container: List[Dict[str, Any]],
        segments: Optional[Dict[str, Any]],
        period: Dict[str, str],
        base_dimensions: Dict[str, Any],
        command_name: str,
        raw_output_path: Optional[str],
        *,
        dimension_key: str,
    ) -> None:
        if not isinstance(segments, dict):
            return

        for segment_name, data in segments.items():
            if not isinstance(data, dict):
                continue
            counts = data.get("counts") if isinstance(data.get("counts"), dict) else {}
            segment_dimensions = {**base_dimensions, dimension_key: segment_name}
            IssueAdherenceCommand._append_metric(
                container,
                "jira.issue_adherence.adherence_rate",
                data.get("adherence_rate"),
                "percent",
                period,
                segment_dimensions,
                command_name,
                raw_output_path,
            )
            IssueAdherenceCommand._append_metric(
                container,
                "jira.issue_adherence.on_time_completion_count",
                counts.get("on_time"),
                "issues",
                period,
                segment_dimensions,
                command_name,
                raw_output_path,
            )
            IssueAdherenceCommand._append_metric(
                container,
                "jira.issue_adherence.late_completion_count",
                counts.get("late"),
                "issues",
                period,
                segment_dimensions,
                command_name,
                raw_output_path,
            )

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

        team_value = IssueAdherenceCommand._normalize_team_metadata(metadata)
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
