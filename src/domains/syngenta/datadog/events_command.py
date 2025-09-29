from __future__ import annotations

import os
from argparse import ArgumentParser, Namespace
from typing import Dict, List, Any

from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded, ensure_datadog_env_loaded
from utils.logging.logging_manager import LogManager
from utils.output_manager import OutputManager

from domains.syngenta.datadog.datadog_events_service import (
    DatadogEventsAuthError,
    DatadogEventsService,
    DatadogEventsServiceError,
)
from domains.syngenta.datadog.enhanced_report_renderer import EnhancedReportRenderer


class EventsCommand(BaseCommand):
    """Query Datadog Events API for monitor alerts grouped by team."""

    @staticmethod
    def get_name() -> str:
        return "events"

    @staticmethod
    def get_description() -> str:
        return "Retrieve Datadog monitor alerts for the specified teams."

    @staticmethod
    def get_help() -> str:
        return (
            "Queries Datadog Events Search API for monitor alerts tagged with the given team "
            "handles, summarizing alert activity across teams with optional JSON or Markdown output."
        )

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--teams",
            type=str,
            required=True,
            help=(
                "Comma-separated list of Datadog team handles, e.g. "
                "'cropwise-core-services-catalog,cropwise-core-services-identity'."
            ),
        )
        parser.add_argument(
            "--days",
            type=int,
            default=3,
            help="Lookback window in days (default: 3).",
        )
        parser.add_argument(
            "--env",
            type=str,
            default="prod",
            help="Environment tag to filter events (default: 'prod').",
        )
        parser.add_argument(
            "--output-format",
            type=str,
            choices=["console", "json", "md"],
            default="console",
            help="Output format for detailed results (default: console).",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Print verbose details about fetched events and caching usage.",
        )
        parser.add_argument(
            "--use-cache",
            action="store_true",
            help="Enable 30-minute caching for Datadog API responses.",
        )
        parser.add_argument(
            "--advanced-analysis",
            action="store_true",
            help="Generate advanced alert quality metrics and removal recommendations.",
        )
        parser.add_argument(
            "--min-confidence",
            type=float,
            default=0.8,
            help="Minimum confidence score for removal recommendations (0.0-1.0, default: 0.8).",
        )
        parser.add_argument(
            "--analysis-period",
            type=int,
            default=30,
            help="Analysis period in days for calculating metrics (default: 30).",
        )
        parser.add_argument(
            "--detailed-stats",
            action="store_true",
            help="Include detailed per-monitor statistics with health scores and recommendations.",
        )
        parser.add_argument(
            "--include-monitors",
            action="store_true",
            help="Include all monitors from teams (active and unused) in the output.",
        )
        parser.add_argument(
            "--unused-only",
            action="store_true",
            help="Show only monitors that haven't triggered in the specified period.",
        )
        parser.add_argument(
            "--show-team-alerts",
            action="store_true",
            help="Include detailed team alert events in markdown output (default: false).",
        )

    @staticmethod
    def main(args: Namespace):
        ensure_env_loaded()
        ensure_datadog_env_loaded()
        logger = LogManager.get_instance().get_logger("EventsCommand")

        try:
            teams = EventsCommand._parse_teams(args.teams)
            if not teams:
                raise ValueError("No valid team handles provided via --teams.")

            days = EventsCommand._validate_days(args.days)
            env = (args.env or "prod").strip() or "prod"

            service = DatadogEventsService(
                site=os.getenv("DD_SITE") or "datadoghq.eu",
                api_key=os.getenv("DD_API_KEY"),
                app_key=os.getenv("DD_APP_KEY"),
                use_cache=bool(args.use_cache),
                cache_ttl_minutes=30,
            )

            result = service.fetch_events_for_teams(teams=teams, env=env, days=days)
            summary = service.generate_summary(
                result["teams"], requested_teams=teams, days=days, env=env
            )

            payload: Dict[str, object] = {
                "summary": summary,
                "teams": result["teams"],
                "metadata": result["metadata"],
            }

            # Add monitors analysis if requested
            if getattr(args, "include_monitors", False) or getattr(args, "unused_only", False):
                monitors_data = service.find_unused_monitors(
                    teams=teams,
                    days=days,
                    env=env,
                    include_all=not getattr(args, "unused_only", False),
                    detailed=False
                )
                payload["monitors"] = monitors_data

            # Add advanced analysis if requested
            if getattr(args, "advanced_analysis", False):
                analysis_period = getattr(args, "analysis_period", 30)
                min_confidence = getattr(args, "min_confidence", 0.8)
                include_detailed_stats = getattr(args, "detailed_stats", False)

                # Get current monitors to detect deleted ones
                current_monitors_data = service.find_unused_monitors(
                    teams=teams,
                    days=days,
                    env=env,
                    include_all=True,
                    detailed=False
                )

                advanced_results = service.analyze_events_advanced(
                    result["teams"],
                    analysis_period_days=analysis_period,
                    min_confidence=min_confidence,
                    include_detailed_stats=include_detailed_stats,
                    existing_monitors=current_monitors_data.get("all_monitors", [])
                )
                payload["advanced_analysis"] = advanced_results

            EventsCommand._print_executive_summary(summary, result["metadata"])

            # Print advanced analysis summary if available
            if "advanced_analysis" in payload:
                EventsCommand._print_advanced_summary(payload["advanced_analysis"])

            # Print monitors analysis if available
            if "monitors" in payload:
                EventsCommand._print_monitors_summary(payload["monitors"], args)

            if getattr(args, "verbose", False):
                EventsCommand._print_verbose_snapshot(payload)

            EventsCommand._handle_output(payload, output_format=args.output_format, args=args)

        except ValueError as exc:
            logger.error("Invalid arguments: %s", exc)
            print("❌ Invalid input. Use --help for usage guidance.")
            print(f"Error: {exc}")
            exit(2)
        except DatadogEventsAuthError as exc:
            logger.error("Datadog authentication failure: %s", exc)
            print("❌ Authentication to Datadog Events API failed.")
            print("Hints:")
            print("- Verify DD_API_KEY and DD_APP_KEY values")
            print("- Ensure keys have access to Events Search API")
            exit(1)
        except DatadogEventsServiceError as exc:
            logger.error("Datadog Events command failed: %s", exc)
            print("❌ Failed to retrieve Datadog events.")
            print(f"Error: {exc}")
            print("Hints:")
            print("- Validate team handles and environment tags")
            print("- Check network access to api.<site>")
            print("- Re-run with --verbose for additional diagnostics")
            exit(1)
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("Unexpected failure: %s", exc, exc_info=True)
            print("❌ Unexpected error while executing Datadog events command.")
            print(f"Error: {exc}")
            exit(1)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_teams(raw: str) -> List[str]:
        return [team.strip() for team in (raw or "").split(",") if team and team.strip()]

    @staticmethod
    def _validate_days(days: int) -> int:
        if days is None:
            return 3
        if days <= 0:
            raise ValueError("--days must be a positive integer.")
        if days > 30:
            raise ValueError("--days lookback above 30 is not supported for this command.")
        return days

    @staticmethod
    def _print_executive_summary(summary: Dict[str, object], metadata: Dict[str, object]) -> None:
        period = summary.get("time_period", {}) if isinstance(summary, dict) else {}
        label = period.get("label", "recent period")
        header = f"🚨 DATADOG EVENTS - Teams Alert Summary ({label})"
        print(f"\n{header}")
        print("EXECUTIVE SUMMARY:\n")

        totals = summary.get("totals", {}) if isinstance(summary, dict) else {}
        total_events = totals.get("events", 0)
        teams_with_alerts = totals.get("teams_with_alerts", 0)
        total_teams = totals.get("total_teams", 0)
        most_active = summary.get("most_active_team", {}) if isinstance(summary, dict) else {}
        most_active_team = most_active.get("team")
        most_active_count = most_active.get("event_count", 0)
        relative_label = period.get("relative_label", "") or period.get("relative", "")

        print(f"Total Events: {total_events}")
        print(f"Teams with Alerts: {teams_with_alerts}/{total_teams} teams")
        if most_active_team:
            print(f"Most Active: {most_active_team} ({most_active_count} events)")
        else:
            print("Most Active: n/a (0 events)")
        if relative_label:
            print(f"Time Period: {relative_label}")
        site = metadata.get("site") if isinstance(metadata, dict) else None
        env = summary.get("env") if isinstance(summary, dict) else None
        if site:
            print(f"Site: {site}")
        if env:
            print(f"Environment: {env}")

    @staticmethod
    def _clean_monitor_name_for_markdown(monitor_name: str, max_length: int = 30) -> str:
        """Clean monitor name for safe use in Markdown tables and links."""
        if not monitor_name:
            return "Unknown"

        # Replace problematic Markdown characters
        clean_name = (monitor_name
                     .replace("|", "│")          # Replace pipe with similar Unicode character
                     .replace("[", "⟨")          # Replace left bracket with similar Unicode character
                     .replace("]", "⟩")          # Replace right bracket with similar Unicode character
                     .replace("(", "❨")          # Replace left paren with similar Unicode character
                     .replace(")", "❩")          # Replace right paren with similar Unicode character
                     .replace("#", "＃")         # Replace hash with full-width character
                     .replace("*", "✱")          # Replace asterisk with similar Unicode character
                     .replace("_", "‿")          # Replace underscore with similar Unicode character
                     .replace("`", "ˋ")          # Replace backtick with similar Unicode character
                     .replace("~", "∼")          # Replace tilde with similar Unicode character
                     .replace("\\", "⧵")        # Replace backslash with similar Unicode character
                     .replace("\n", " ")          # Replace newlines with spaces
                     .replace("\r", " ")          # Replace carriage returns with spaces
                     .replace("\t", " ")          # Replace tabs with spaces
                     .strip())                    # Remove leading/trailing whitespace

        # Collapse multiple spaces into single spaces
        clean_name = " ".join(clean_name.split())

        # Truncate if too long
        if len(clean_name) > max_length:
            return clean_name[:max_length] + "..."

        return clean_name

    @staticmethod
    def _create_monitor_link(monitor_name: str, monitor_id: str = None, max_length: int = 40, is_deleted: bool = False, deleted_monitors: set = None) -> str:
        """Create a markdown link for a monitor.

        Args:
            monitor_name: Display name of the monitor
            monitor_id: Monitor ID for the link (optional)
            max_length: Maximum length for display name
            is_deleted: Whether the monitor is deleted (adds strikethrough)
            deleted_monitors: Set of deleted monitor IDs to check against

        Returns:
            Markdown formatted link or plain name if no ID available
        """
        import os

        # Clean the name for display
        clean_name = EventsCommand._clean_monitor_name_for_markdown(monitor_name, max_length)

        # Check if monitor is deleted (from parameter or from deleted_monitors set)
        is_monitor_deleted = is_deleted
        if not is_monitor_deleted and deleted_monitors and monitor_id:
            is_monitor_deleted = str(monitor_id) in deleted_monitors

        # Create link if we have monitor ID
        if monitor_id and monitor_id != "Unknown" and str(monitor_id).strip():
            datadog_site = os.getenv("DD_SITE", "app.datadoghq.com")
            datadog_base_url = f"https://app.{datadog_site}"
            link = f"[{clean_name}]({datadog_base_url}/monitors/{monitor_id})"

            # Add strikethrough and emoji for deleted monitors
            if is_monitor_deleted:
                link = f"🗑️ ~~{link}~~"

            return link

        # For plain names, still mark deleted ones
        if is_monitor_deleted:
            return f"🗑️ ~~{clean_name}~~"

        return clean_name

    @staticmethod
    def _print_advanced_summary(advanced_analysis: object) -> None:
        if not isinstance(advanced_analysis, dict):
            return

        print("\n🔍 ADVANCED ANALYSIS SUMMARY:")

        # Alert Quality Summary
        alert_quality = advanced_analysis.get("alert_quality", {})
        if isinstance(alert_quality, dict):
            overall = alert_quality.get("overall", {})
            if isinstance(overall, dict):
                noise_score = overall.get("overall_noise_score", 0)
                self_healing = overall.get("self_healing_rate", 0)
                total_monitors = overall.get("total_monitors", 0)
                actionable_pct = overall.get("actionable_alerts_percentage", 0)

                print("Alert Quality Metrics:")
                print(f"  • Overall Noise Score: {noise_score}/100")
                print(f"  • Self-Healing Rate: {self_healing:.1%}")
                print(f"  • Total Monitors Analyzed: {total_monitors}")
                print(f"  • Actionable Alerts: {actionable_pct:.1%}")

        # Removal Candidates Summary
        removal_candidates = advanced_analysis.get("removal_candidates", {})
        if isinstance(removal_candidates, dict):
            candidates_count = removal_candidates.get("count", 0)
            estimated_reduction = removal_candidates.get("estimated_noise_reduction", 0)
            print("\nRemoval Recommendations:")
            print(f"  • Candidates for Removal: {candidates_count}")
            print(f"  • Estimated Noise Reduction: {estimated_reduction:.1%}")

        # Top Removal Candidates
        candidates_list = removal_candidates.get("items", []) if isinstance(removal_candidates, dict) else []
        if candidates_list and isinstance(candidates_list, list):
            print("\nTop Removal Candidates:")
            for i, candidate in enumerate(candidates_list[:3]):
                if isinstance(candidate, dict):
                    monitor_name = candidate.get("monitor_name") or candidate.get("monitor_id", "Unknown")
                    confidence = candidate.get("confidence_score", 0)
                    noise_score = candidate.get("noise_score", 0)
                    print(f"  {i+1}. {monitor_name}")
                    print(f"     Confidence: {confidence:.2f}, Noise: {noise_score:.1f}/100")

        # Temporal Metrics
        temporal = advanced_analysis.get("temporal_metrics", {})
        if isinstance(temporal, dict):
            ttr = temporal.get("avg_time_to_resolution_minutes")
            mtbf = temporal.get("mtbf_hours")
            if ttr is not None or mtbf is not None:
                print("\nTemporal Metrics:")
                if ttr is not None:
                    print(f"  • Avg Time to Resolution: {ttr:.1f} minutes")
                if mtbf is not None:
                    print(f"  • Mean Time Between Failures: {mtbf:.1f} hours")

        # Detailed Monitor Statistics Summary
        detailed_stats = advanced_analysis.get("detailed_monitor_statistics", {})
        if isinstance(detailed_stats, dict):
            overall_insights = detailed_stats.get("overall_insights", {})
            if isinstance(overall_insights, dict):
                total_monitors = overall_insights.get("total_monitors_analyzed", 0)
                avg_health = overall_insights.get("average_health_score")
                needing_attention = overall_insights.get("monitors_needing_attention", 0)

                if total_monitors > 0:
                    print("\nMonitor Health Overview:")
                    print(f"  • Monitors Analyzed: {total_monitors}")
                    if avg_health is not None:
                        print(f"  • Average Health Score: {avg_health:.1f}/100")
                    print(f"  • Monitors Needing Attention: {needing_attention}")

                    # Grade distribution
                    grades = overall_insights.get("grade_distribution", {})
                    if grades:
                        grade_summary = ", ".join([f"{grade}: {count}" for grade, count in grades.items()])
                        print(f"  • Health Grades: {grade_summary}")

            # Enhanced recommendations summary
            recommendations = detailed_stats.get("recommendations", {})
            if isinstance(recommendations, dict):
                high_priority = recommendations.get("high_priority_removals", [])
                automation_candidates = recommendations.get("automation_candidates", [])

                if high_priority:
                    print("\nHigh-Priority Actions:")
                    print(f"  • High-Priority Removals: {len(high_priority)} monitors")

                if automation_candidates:
                    print(f"  • Automation Opportunities: {len(automation_candidates)} monitors")

    @staticmethod
    def _print_monitors_summary(monitors_data: Dict[str, Any], args: Namespace) -> None:
        """Print monitors analysis summary"""
        if not monitors_data:
            return

        summary = monitors_data.get("summary", {})
        unused_monitors = monitors_data.get("unused_monitors", [])
        active_monitors = monitors_data.get("active_monitors", [])

        print("\n" + "=" * 60)
        print("📊 MONITORS ANALYSIS")
        print("=" * 60)

        # Print summary stats
        total_monitors = summary.get("total_monitors", 0)
        unused_count = summary.get("unused_monitors", 0)
        active_count = summary.get("active_monitors", 0)
        analysis_days = summary.get("analysis_period_days", 0)

        print(f"Analysis Period: Last {analysis_days} days")
        print(f"Total Monitors Found: {total_monitors}")
        print(f"  ✅ Active Monitors: {active_count}")
        print(f"  📵 Unused Monitors: {unused_count}")
        print(f"  🔇 Muted Monitors: {summary.get('muted_monitors', 0)}")

        # Group monitors by team for better organization
        monitors_by_team = {}

        # Process unused monitors
        for monitor in unused_monitors:
            team_tags = [tag.split("team:", 1)[1] for tag in monitor.get("tags", []) if tag.startswith("team:")]
            for team in team_tags:
                if team not in monitors_by_team:
                    monitors_by_team[team] = {"active": [], "unused": []}
                monitors_by_team[team]["unused"].append(monitor)

        # Process active monitors (if included)
        if active_monitors:
            for monitor in active_monitors:
                team_tags = [tag.split("team:", 1)[1] for tag in monitor.get("tags", []) if tag.startswith("team:")]
                for team in team_tags:
                    if team not in monitors_by_team:
                        monitors_by_team[team] = {"active": [], "unused": []}
                    monitors_by_team[team]["active"].append(monitor)

        # Print by team
        for team in sorted(monitors_by_team.keys()):
            team_data = monitors_by_team[team]
            team_active = len(team_data["active"])
            team_unused = len(team_data["unused"])

            print(f"\n🏷️  Team: {team}")
            print(f"   Active: {team_active} | Unused: {team_unused}")

            # Show unused monitors (always show these)
            if team_unused > 0:
                print("   📵 Unused Monitors:")
                for monitor in team_data["unused"][:10]:  # Show first 10
                    days_ago = monitor.get("last_triggered_days_ago")
                    days_text = f"{days_ago}d ago" if days_ago else "Never"
                    status_icon = "🔇" if monitor.get("muted") else "📵"
                    print(f"     {status_icon} {monitor.get('name', 'Unknown')[:60]} (Last: {days_text})")

                if team_unused > 10:
                    print(f"     ... and {team_unused - 10} more unused monitors")

            # Show active monitors only if --include-monitors is specified
            if getattr(args, "include_monitors", False) and team_active > 0:
                print("   ✅ Recent Active Monitors (sample):")
                for monitor in team_data["active"][:5]:  # Show first 5
                    days_ago = monitor.get("last_triggered_days_ago", 999)
                    days_text = f"{days_ago}d ago" if days_ago < 999 else "Today"
                    print(f"     ✅ {monitor.get('name', 'Unknown')[:60]} (Last: {days_text})")

        if unused_count == 0:
            print(f"\n🎉 Excellent! All monitors have been active in the last {analysis_days} days.")
        elif unused_count > 0:
            print(f"\n💡 Consider reviewing the {unused_count} unused monitors for potential removal or threshold adjustment.")

    @staticmethod
    def _print_verbose_snapshot(payload: Dict[str, object]) -> None:
        teams = payload.get("teams", {}) if isinstance(payload, dict) else {}
        metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
        monitors_data = payload.get("monitors", {}) if isinstance(payload, dict) else {}
        error_notes = metadata.get("errors", {}) if isinstance(metadata, dict) else {}

        print("\n" + "=" * 60)
        print("📋 VERBOSE PROCESSING DETAILS")
        print("=" * 60)

        # Show data processing summary
        if isinstance(monitors_data, dict) and monitors_data:
            summary = monitors_data.get("summary", {})
            print("\n🔍 Data Processing Summary:")
            print("  • Total API Calls: Monitor Search + Events Fetch")
            print(f"  • Monitors Discovery: {summary.get('total_monitors', 0)} monitors found")
            print("  • Events Analysis: Processing events to identify active monitors")
            print("  • Classification: Active vs. Unused monitor determination")
            print(f"  • Environment: {summary.get('env', 'N/A')}")
            print(f"  • Analysis Period: {summary.get('analysis_period_days', 0)} days")

            # Show monitor breakdown by team
            unused_monitors = monitors_data.get("unused_monitors", [])
            active_monitors = monitors_data.get("active_monitors", [])

            monitors_by_team = {}

            # Group unused monitors by team
            for monitor in unused_monitors:
                team_tags = [tag.split("team:", 1)[1] for tag in monitor.get("tags", []) if tag.startswith("team:")]
                for team in team_tags:
                    if team not in monitors_by_team:
                        monitors_by_team[team] = {"active": 0, "unused": 0}
                    monitors_by_team[team]["unused"] += 1

            # Group active monitors by team
            for monitor in active_monitors:
                team_tags = [tag.split("team:", 1)[1] for tag in monitor.get("tags", []) if tag.startswith("team:")]
                for team in team_tags:
                    if team not in monitors_by_team:
                        monitors_by_team[team] = {"active": 0, "unused": 0}
                    monitors_by_team[team]["active"] += 1

            if monitors_by_team:
                print("\n📊 Monitor Distribution by Team:")
                for team in sorted(monitors_by_team.keys()):
                    team_data = monitors_by_team[team]
                    total = team_data["active"] + team_data["unused"]
                    unused_pct = (team_data["unused"] / max(total, 1)) * 100
                    print(f"  • {team}: {total} total ({team_data['active']} active, {team_data['unused']} unused - {unused_pct:.1f}% unused)")

        # Show events processing details
        if not teams:
            print("\n⚠️  No team event data available")
            return

        print("\n📈 Events Processing Details:")
        total_events = 0
        for team, data in teams.items():
            event_count = data.get("event_count", 0) if isinstance(data, dict) else 0
            total_events += event_count

        print(f"  • Total Events Processed: {total_events} across {len(teams)} teams")
        print(f"  • Average Events per Team: {total_events / len(teams):.1f}")

        print("\n📋 Team Event Summary (showing first 3 events per team):")
        for team, data in teams.items():
            event_count = data.get("event_count", 0) if isinstance(data, dict) else 0
            print(f"- {team}: {event_count} events")
            if data.get("error"):
                print(f"  ⚠️ Error: {data['error']}")
                continue
            events = data.get("events", []) if isinstance(data, dict) else []
            for event in events[:3]:
                timestamp = event.get("timestamp")
                title = event.get("title")
                alert_type = event.get("alert_type")
                print(f"    • {timestamp} | {alert_type or 'alert'} | {title}")

        if error_notes:
            print("\n❌ API Errors Encountered:")
            for team, error in error_notes.items():
                print(f"  - {team}: {error}")

        # Show cache usage information
        print("\n💾 Cache Usage:")
        print("  • Monitors: Cached for 30 minutes to improve performance")
        print("  • Events: Cached for 30 minutes to reduce API calls")
        print("  • Use --clear-cache to force fresh API data")

        print("\n" + "=" * 60)

    @staticmethod
    def _handle_output(payload: Dict[str, object], *, output_format: str, args: Namespace) -> None:
        if output_format == "console":
            return

        from datetime import datetime as _dt

        sub_dir = f"datadog-events_{_dt.now().strftime('%Y%m%d')}"
        base_name = "datadog_events"

        if output_format == "json":
            output_path = OutputManager.save_json_report(payload, sub_dir, base_name)
            print("\nOutput file:")
            print(f"- {output_path}")
            print("✅ Detailed events payload saved in JSON format")
            return

        markdown = EventsCommand._to_markdown(payload, args)
        output_path = OutputManager.save_markdown_report(markdown, sub_dir, base_name)
        print("\nOutput file:")
        print(f"- {output_path}")
        print("📄 Markdown report saved")

    @staticmethod
    def _to_markdown(payload: Dict[str, object], args: Namespace) -> str:
        summary = payload.get("summary", {}) if isinstance(payload, dict) else {}
        metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
        teams = payload.get("teams", {}) if isinstance(payload, dict) else {}
        advanced_analysis = payload.get("advanced_analysis", {}) if isinstance(payload, dict) else {}

        # Extract deleted monitors from advanced analysis if available
        deleted_monitors = set()
        if isinstance(advanced_analysis, dict):
            analyzer_instance = advanced_analysis.get("_analyzer")
            if hasattr(analyzer_instance, 'deleted_monitors'):
                deleted_monitors = analyzer_instance.deleted_monitors
            # Fallback: extract from detailed stats
            elif "detailed_monitor_statistics" in advanced_analysis:
                detailed_stats = advanced_analysis["detailed_monitor_statistics"]
                if isinstance(detailed_stats, dict) and "per_monitor" in detailed_stats:
                    per_monitor = detailed_stats["per_monitor"]
                    if isinstance(per_monitor, dict):
                        for monitor_id, stats in per_monitor.items():
                            if isinstance(stats, dict) and stats.get("is_deleted", False):
                                deleted_monitors.add(str(monitor_id))

        totals = summary.get("totals", {}) if isinstance(summary, dict) else {}
        period = summary.get("time_period", {}) if isinstance(summary, dict) else {}
        lines: List[str] = []
        lines.append("## Datadog Monitor Alerts Report")
        lines.append("")
        site = metadata.get("site") if isinstance(metadata, dict) else None
        env = summary.get("env") if isinstance(summary, dict) else None
        lookback = period.get("relative_label") or period.get("relative")
        label = period.get("label")
        generated_at = metadata.get("generated_at")

        if site:
            lines.append(f"- Site: `{site}`")
        if env:
            lines.append(f"- Environment: `{env}`")
        if lookback:
            lines.append(f"- Lookback: {lookback}")
        if label:
            lines.append(f"- Period: {label}")
        if generated_at:
            lines.append(f"- Generated at: {generated_at}")
        lines.append("")

        lines.append("### Executive Summary")
        lines.append("")
        lines.append(f"- Total Events: {totals.get('events', 0)}")
        lines.append(
            f"- Teams with Alerts: {totals.get('teams_with_alerts', 0)}/{totals.get('total_teams', 0)}"
        )
        most_active = summary.get("most_active_team", {}) if isinstance(summary, dict) else {}
        if most_active.get("team"):
            lines.append(
                f"- Most Active: {most_active.get('team')} ({most_active.get('event_count', 0)} events)"
            )
        else:
            lines.append("- Most Active: n/a (0 events)")
        lines.append("")

        # Add advanced analysis section if available
        if isinstance(advanced_analysis, dict) and advanced_analysis:
            lines.append("### Advanced Analysis")
            lines.append("")
            lines.append("This section provides comprehensive insights into alert quality, monitor behavior, and actionable recommendations for optimizing your monitoring strategy.")
            lines.append("")

            # Alert Quality Metrics
            alert_quality = advanced_analysis.get("alert_quality", {})
            if isinstance(alert_quality, dict):
                overall = alert_quality.get("overall", {})
                if isinstance(overall, dict):
                    lines.append("#### Alert Quality Metrics")
                    lines.append("")
                    lines.append("**Quality metrics help identify the overall health of your monitoring system:**")
                    lines.append("")
                    lines.append(f"- **Overall Noise Score**: {overall.get('overall_noise_score', 0):.1f}/100")
                    lines.append("  - *Lower scores indicate cleaner, more actionable alerts*")
                    lines.append(f"- **Self-Healing Rate**: {overall.get('self_healing_rate', 0):.1%}")
                    lines.append("  - *Percentage of alerts that resolve automatically without manual intervention*")
                    lines.append(f"- **Total Monitors Analyzed**: {overall.get('total_monitors', 0)}")
                    lines.append(f"- **Actionable Alerts**: {overall.get('actionable_alerts_percentage', 0):.1%}")
                    lines.append("  - *Percentage of alerts that typically require human action*")

                    # Add enhanced classification metrics if available
                    enhanced_analysis = advanced_analysis.get("enhanced_analysis", {})
                    if isinstance(enhanced_analysis, dict):
                        classification_summary = enhanced_analysis.get("classification_summary", {})
                        if isinstance(classification_summary, dict) and classification_summary.get("total_cycles", 0) > 0:
                            total_cycles = classification_summary.get("total_cycles", 0)
                            flapping = classification_summary.get("flapping_cycles", 0)
                            benign = classification_summary.get("benign_transient_cycles", 0)
                            actionable = classification_summary.get("actionable_cycles", 0)
                            confidence = classification_summary.get("avg_confidence", 0)

                            lines.append("")
                            lines.append("**🔬 Alert Behavior Classification:**")
                            lines.append(f"- **🔄 Flapping Alerts**: {flapping} ({flapping/max(total_cycles,1)*100:.1f}%)")
                            lines.append("  - *Rapid state oscillations indicating threshold issues*")
                            lines.append(f"- **⚡ Benign Transients**: {benign} ({benign/max(total_cycles,1)*100:.1f}%)")
                            lines.append("  - *Short-lived, self-resolving issues requiring no action*")
                            lines.append(f"- **🎯 Actionable Alerts**: {actionable} ({actionable/max(total_cycles,1)*100:.1f}%)")
                            lines.append("  - *Legitimate alerts requiring human intervention*")
                            lines.append(f"- **Classification Confidence**: {confidence:.1%}")

                            if confidence >= 0.8:
                                lines.append("  - 🟢 *High confidence - classifications are reliable*")
                            elif confidence >= 0.6:
                                lines.append("  - 🟡 *Medium confidence - review edge cases*")
                            else:
                                lines.append("  - 🔴 *Low confidence - may need threshold tuning*")

                            # Add explanation about Events vs Cycles
                            total_events = totals.get('events', 0)
                            lines.append("")
                            lines.append(f"📊 **Events vs Cycles Breakdown:**")
                            lines.append(f"- **Total Events**: {total_events} individual Datadog events")
                            lines.append(f"- **Alert Cycles**: {total_cycles} complete alert→recovery sequences")
                            if total_events > total_cycles:
                                events_per_cycle = total_events / max(total_cycles, 1)
                                lines.append(f"- **Events per Cycle**: {events_per_cycle:.1f} average")
                                lines.append("- *Note: Multiple events can form one cycle (alert start, recovery, notifications)*")

                            lines.append("")
                            lines.append("**📖 Classification Methodology:**")
                            lines.append("")
                            lines.append("*Our system automatically analyzes alert cycles and classifies them based on behavior patterns:*")
                            lines.append("")
                            lines.append("- **🔄 FLAPPING**: Alerts that rapidly oscillate between states")
                            lines.append("  - Detected when: ≥3 cycles within 60min OR ≥4 state transitions in one cycle")
                            lines.append("  - Root cause: Usually threshold too sensitive or system instability")
                            lines.append("  - Fix: Increase debounce window, add hysteresis, or adjust thresholds")
                            lines.append("  - *Example*: CPU alert toggles OK→ALERT→OK every 2 minutes for 1 hour")
                            lines.append("")
                            lines.append("- **⚡ BENIGN_TRANSIENT**: Short-lived issues that resolve automatically")
                            lines.append("  - Detected when: Duration ≤5min AND simple alert→recovery AND no manual action")
                            lines.append("  - Root cause: Temporary upstream blips, brief network issues, transient load spikes")
                            lines.append("  - Fix: Route to dashboard-only, increase alert duration threshold, or add context")
                            lines.append("  - *Example*: 502 errors spike for 3 minutes during AWS deployment, then auto-recover")
                            lines.append("")
                            lines.append("- **🎯 ACTIONABLE**: Legitimate alerts requiring human attention")
                            lines.append("  - Detected when: Duration ≥10min OR evidence of manual intervention OR complex patterns")
                            lines.append("  - Root cause: Real system issues, performance problems, or service outages")
                            lines.append("  - Action: Keep as-is, optimize response procedures, improve runbooks")
                            lines.append("  - *Example*: Database connection pool exhausted for 45 minutes until DBA increases pool size")
                            lines.append("")
                            lines.append("*Classification uses Brazilian business hours (9 AM - 5 PM BRT/BRST) for context and considers alert correlation patterns.*")

                    lines.append("")

            # Removal Candidates
            removal_candidates = advanced_analysis.get("removal_candidates", {})
            if isinstance(removal_candidates, dict):
                candidates_list = removal_candidates.get("items", [])
                if isinstance(candidates_list, list) and candidates_list:
                    lines.append("#### Top Removal Candidates")
                    lines.append("")
                    lines.append("**Monitors identified as potential candidates for removal or silencing based on data analysis:**")
                    lines.append("")
                    lines.append("- **Confidence Score**: How certain we are about the recommendation (0.0-1.0)")
                    lines.append("- **Noise Score**: Composite score indicating alert noisiness (higher = noisier)")
                    lines.append("- **Reasons**: Data-driven factors supporting the recommendation")
                    lines.append("")
                    lines.append("| Monitor | Confidence | Noise Score | Reasons |")
                    lines.append("|---------|------------|-------------|---------|")
                    # Get Datadog site URL for links in removal candidates table
                    datadog_site = os.getenv("DD_SITE", "app.datadoghq.com")
                    datadog_base_url = f"https://app.{datadog_site}"

                    for candidate in candidates_list[:5]:
                        if isinstance(candidate, dict):
                            monitor_name = candidate.get("monitor_name") or candidate.get("monitor_id", "Unknown")
                            monitor_id = candidate.get("monitor_id")
                            confidence = candidate.get("confidence_score", 0)
                            noise_score = candidate.get("noise_score", 0)
                            reasons = candidate.get("reasons", [])
                            reason_text = "; ".join(reasons) if isinstance(reasons, list) else ""

                            # Create monitor link with deletion check
                            monitor_display = EventsCommand._create_monitor_link(monitor_name, monitor_id, 25, deleted_monitors=deleted_monitors)

                            # Also clean reason text
                            clean_reason_text = reason_text.replace("|", "│")
                            lines.append(f"| {monitor_display} | {confidence:.2f} | {noise_score:.1f} | {clean_reason_text} |")
                    lines.append("")
                    lines.append("💡 **Tip**: Start with monitors having confidence > 0.85 for safe removal candidates.")
                    lines.append("")

            # Temporal Metrics
            temporal = advanced_analysis.get("temporal_metrics", {})
            if isinstance(temporal, dict):
                ttr = temporal.get("avg_time_to_resolution_minutes")
                alert_dur = temporal.get("avg_alert_duration_minutes")
                mtbf = temporal.get("mtbf_hours")

                if any(x is not None for x in [ttr, alert_dur, mtbf]):
                    lines.append("#### Temporal Metrics")
                    lines.append("")
                    lines.append("**Time-based analysis reveals patterns in alert lifecycle and system stability:**")
                    lines.append("")
                    if ttr is not None:
                        lines.append(f"- **Average Time to Resolution**: {ttr:.1f} minutes")
                        lines.append("  - *How long it takes from first alert to final recovery*")
                    if alert_dur is not None:
                        lines.append(f"- **Average Alert Duration**: {alert_dur:.1f} minutes")
                        lines.append("  - *Time spent in critical alert state*")
                    if mtbf is not None:
                        lines.append(f"- **Mean Time Between Failures**: {mtbf:.1f} hours")
                        lines.append("  - *Average time between separate alert cycles (higher is better)*")
                    lines.append("")

                    # Add interpretation guidance
                    if ttr is not None:
                        if ttr < 5:
                            lines.append("🟢 **Quick resolution times** suggest either effective response or potentially noisy alerts")
                        elif ttr > 60:
                            lines.append("🟡 **Long resolution times** may indicate complex issues or delayed responses")

                    # Add trend analysis if available
                    trends = advanced_analysis.get("trends")
                    if isinstance(trends, dict) and trends.get("summary"):
                        trend_summary = trends.get("summary", {})
                        if isinstance(trend_summary, dict):
                            analysis_week = trend_summary.get("analysis_week")
                            weeks_available = trend_summary.get("weeks_available", 0)

                            if weeks_available >= 3:  # Only show if we have sufficient data
                                lines.append("")
                                lines.append("**📈 Week-over-Week Trends:**")

                                improving = trend_summary.get("monitors_improving", 0)
                                degrading = trend_summary.get("monitors_degrading", 0)
                                stable = trend_summary.get("monitors_stable", 0)
                                total_trend_monitors = improving + degrading + stable

                                if total_trend_monitors > 0:
                                    lines.append(f"- 🟢 **Improving**: {improving} monitors ({improving/total_trend_monitors*100:.1f}%)")
                                    lines.append(f"- 🔴 **Degrading**: {degrading} monitors ({degrading/total_trend_monitors*100:.1f}%)")
                                    lines.append(f"- ⚪ **Stable**: {stable} monitors ({stable/total_trend_monitors*100:.1f}%)")
                                    lines.append(f"- 📊 **Analysis Period**: Week {analysis_week} ({weeks_available} weeks of data)")

                                # Add significant changes
                                significant_changes = trend_summary.get("significant_changes", [])
                                if significant_changes and isinstance(significant_changes, list):
                                    lines.append("- **Notable Changes This Week:**")
                                    for change in significant_changes[:3]:  # Top 3
                                        lines.append(f"  - {change}")
                            elif weeks_available > 0:
                                lines.append(f"")
                                lines.append(f"📊 **Trend Analysis**: Building historical data ({weeks_available} weeks collected, need 3+ for reliable trends)")

                    lines.append("")

            # Detailed Monitor Statistics Section
            detailed_stats = advanced_analysis.get("detailed_monitor_statistics", {})
            if isinstance(detailed_stats, dict) and detailed_stats:
                lines.append("#### Detailed Monitor Analysis")
                lines.append("")
                lines.append("**Comprehensive per-monitor statistics with health scoring and business impact analysis:**")
                lines.append("")

                # Overall insights
                overall_insights = detailed_stats.get("overall_insights", {})
                if isinstance(overall_insights, dict):
                    total_monitors = overall_insights.get("total_monitors_analyzed", 0)
                    avg_health = overall_insights.get("average_health_score")
                    needing_attention = overall_insights.get("monitors_needing_attention", 0)

                    if total_monitors > 0:
                        lines.append("##### Monitor Health Overview")
                        lines.append("")
                        lines.append(f"- **Total Monitors Analyzed**: {total_monitors}")
                        if avg_health is not None:
                            lines.append(f"- **Average Health Score**: {avg_health:.1f}/100")

                            # Health score interpretation
                            if avg_health >= 80:
                                lines.append("  - 🟢 *Excellent overall health - most monitors are performing well*")
                            elif avg_health >= 60:
                                lines.append("  - 🟡 *Good health with room for improvement*")
                            else:
                                lines.append("  - 🔴 *Poor health - many monitors need attention*")

                        lines.append(f"- **Monitors Needing Attention**: {needing_attention}")
                        if needing_attention > 0:
                            percentage_needing_attention = (needing_attention / total_monitors) * 100
                            lines.append(f"  - *{percentage_needing_attention:.1f}% of monitors have health scores below 60*")

                        # Grade distribution is shown in the detailed analysis section below
                        lines.append("")

                # Enhanced recommendations
                recommendations = detailed_stats.get("recommendations", {})
                if isinstance(recommendations, dict):
                    lines.append("##### Actionable Recommendations")
                    lines.append("")

                    # High priority removals
                    high_priority = recommendations.get("high_priority_removals", [])
                    if high_priority and isinstance(high_priority, list):
                        lines.append("**🔴 High-Priority Removal Candidates:**")
                        lines.append("")
                        lines.append("*These monitors show strong indicators for removal based on poor health scores and high event volume:*")
                        lines.append("")
                        for removal in high_priority[:3]:  # Top 3
                            if isinstance(removal, dict):
                                monitor_name = removal.get("monitor_name") or removal.get("monitor_id", "Unknown")
                                monitor_id = removal.get("monitor_id")
                                monitor_link = EventsCommand._create_monitor_link(monitor_name, monitor_id, 50, deleted_monitors=deleted_monitors)
                                health_score = removal.get("health_score", 0)
                                total_events = removal.get("total_events", 0)
                                reason = removal.get("reason", "")
                                lines.append(f"- **{monitor_link}**")
                                lines.append(f"  - Health Score: {health_score:.1f}/100")
                                lines.append(f"  - Total Events: {total_events}")
                                lines.append(f"  - Reason: *{reason}*")
                        lines.append("")

                    # Automation candidates
                    automation = recommendations.get("automation_candidates", [])
                    if automation and isinstance(automation, list):
                        lines.append("**🤖 Automation Opportunities:**")
                        lines.append("")
                        lines.append("*Monitors with high rates of quick recovery - consider automating the resolution:*")
                        lines.append("")
                        for auto_candidate in automation[:3]:  # Top 3
                            if isinstance(auto_candidate, dict):
                                monitor_name = auto_candidate.get("monitor_name") or auto_candidate.get("monitor_id", "Unknown")
                                monitor_id = auto_candidate.get("monitor_id")
                                monitor_link = EventsCommand._create_monitor_link(monitor_name, monitor_id, 50, deleted_monitors=deleted_monitors)
                                quick_rate = auto_candidate.get("quick_recovery_rate", 0)
                                lines.append(f"- **{monitor_link}**")
                                lines.append(f"  - Quick Recovery Rate: {quick_rate:.1%}")
                                lines.append(f"  - Suggestion: *{auto_candidate.get('suggestion', '')}*")
                        lines.append("")

                    # Threshold adjustments
                    threshold_adj = recommendations.get("threshold_adjustments", [])
                    if threshold_adj and isinstance(threshold_adj, list):
                        lines.append("**⚙️ Threshold Adjustment Candidates:**")
                        lines.append("")
                        lines.append("*Monitors that may benefit from threshold tuning to reduce noise:*")
                        lines.append("")
                        for threshold in threshold_adj[:3]:  # Top 3
                            if isinstance(threshold, dict):
                                monitor_name = threshold.get("monitor_name") or threshold.get("monitor_id", "Unknown")
                                monitor_id = threshold.get("monitor_id")
                                monitor_link = EventsCommand._create_monitor_link(monitor_name, monitor_id, 50, deleted_monitors=deleted_monitors)
                                health_score = threshold.get("health_score", 0)
                                cycles_week = threshold.get("cycles_per_week", 0)
                                lines.append(f"- **{monitor_link}**")
                                lines.append(f"  - Health Score: {health_score:.1f}/100")
                                lines.append(f"  - Alert Cycles per Week: {cycles_week:.1f}")
                                lines.append(f"  - Suggestion: *{threshold.get('suggestion', '')}*")
                        lines.append("")

                    # Enhanced recommendations from classification
                    enhanced_recommendations = advanced_analysis.get("recommendations", {})
                    if isinstance(enhanced_recommendations, dict):
                        # Flapping mitigation recommendations
                        threshold_adjustments = enhanced_recommendations.get("threshold_adjustments", [])
                        if threshold_adjustments and isinstance(threshold_adjustments, list):
                            lines.append("**🔄 Flapping Alert Mitigation:**")
                            lines.append("")
                            lines.append("*Monitors showing rapid state oscillations - implement threshold tuning:*")
                            lines.append("")
                            for rec in threshold_adjustments[:3]:  # Top 3
                                if isinstance(rec, dict):
                                    monitor_name = rec.get("monitor_name", "Unknown")
                                    monitor_id = rec.get("monitor_id")
                                    monitor_link = EventsCommand._create_monitor_link(monitor_name, monitor_id, 40, deleted_monitors=deleted_monitors)
                                    action = rec.get("action", "")
                                    reason = rec.get("reason", "")
                                    lines.append(f"- **{monitor_link}**")
                                    lines.append(f"  - Issue: {reason}")
                                    lines.append(f"  - Action: {action}")
                                    details = rec.get("details", {})
                                    if isinstance(details, dict):
                                        if "suggested_debounce_seconds" in details:
                                            lines.append(f"  - Suggested Debounce: {details['suggested_debounce_seconds']:.0f} seconds")
                                        if details.get("suggested_hysteresis"):
                                            lines.append("  - Consider Hysteresis: Use separate up/down thresholds")
                            lines.append("")

                        # Benign transient policy recommendations
                        benign_policies = enhanced_recommendations.get("benign_transient_policies", [])
                        if benign_policies and isinstance(benign_policies, list):
                            lines.append("**⚡ Benign Transient Policy Changes:**")
                            lines.append("")
                            lines.append("*Monitors with high rates of self-resolving transients - consider routing changes:*")
                            lines.append("")
                            for rec in benign_policies[:3]:  # Top 3
                                if isinstance(rec, dict):
                                    monitor_name = rec.get("monitor_name", "Unknown")
                                    monitor_id = rec.get("monitor_id")
                                    monitor_link = EventsCommand._create_monitor_link(monitor_name, monitor_id, 40, deleted_monitors=deleted_monitors)
                                    action = rec.get("action", "")
                                    reason = rec.get("reason", "")
                                    lines.append(f"- **{monitor_link}**")
                                    lines.append(f"  - Pattern: {reason}")
                                    lines.append(f"  - Action: {action}")
                                    details = rec.get("details", {})
                                    if isinstance(details, dict):
                                        if details.get("consider_dashboard_only"):
                                            lines.append("  - Notification Change: Route to dashboard instead of alerting")
                                        if "suggested_notification_level" in details:
                                            lines.append(f"  - Severity Adjustment: Change to '{details['suggested_notification_level']}' level")
                            lines.append("")

                # Individual Monitor Statistics
                per_monitor = detailed_stats.get("per_monitor", {})
                if isinstance(per_monitor, dict) and per_monitor:
                    lines.append("##### Individual Monitor Statistics")
                    lines.append("")
                    lines.append("**Detailed breakdown of each monitor's performance metrics:**")
                    lines.append("")

                    # Create comprehensive table with full monitor details
                    lines.append("| Monitor Name | Service | Monitor ID | Status | Grade | Health Score | Events | Events/Day | Cycles/Week | Self-Heal Rate | Noise Score | Confidence | TTR (min) | Median Cycle (min) | Business Hours % |")
                    lines.append("|--------------|---------|------------|--------|-------|--------------|--------|------------|-------------|----------------|-------------|------------|-----------|-------------------|------------------|")

                    # Sort monitors by health score (worst first for attention)
                    monitor_items = []
                    for monitor_id, stats in per_monitor.items():
                        if isinstance(stats, dict):
                            health_score = stats.get("health_score", {}).get("score", 0) if isinstance(stats.get("health_score"), dict) else 0
                            monitor_items.append((monitor_id, stats, health_score))

                    monitor_items.sort(key=lambda x: x[2])  # Sort by health score ascending (worst first)

                    # Get Datadog site URL for monitor links
                    datadog_site = os.getenv("DD_SITE", "app.datadoghq.com")
                    datadog_base_url = f"https://app.{datadog_site}"

                    for monitor_id, stats, health_score in monitor_items:
                        monitor_name = stats.get("monitor_name") or monitor_id
                        health_dict = stats.get("health_score", {}) if isinstance(stats.get("health_score"), dict) else {}
                        grade = health_dict.get("grade", "N/A")
                        total_events = stats.get("total_events", 0)
                        events_per_day = stats.get("events_per_day", 0)
                        cycles_per_week = stats.get("cycles_per_week", 0)

                        # Get self-healing rate from the main quality metrics if available
                        self_heal_rate = "N/A"
                        noise_score = "N/A"
                        confidence = "N/A"

                        # Try to find this monitor in the quality metrics
                        if isinstance(alert_quality, dict):
                            quality_per_monitor = alert_quality.get("per_monitor", {})
                            if isinstance(quality_per_monitor, dict) and monitor_id in quality_per_monitor:
                                monitor_quality = quality_per_monitor[monitor_id]
                                if isinstance(monitor_quality, dict):
                                    self_heal_rate = f"{monitor_quality.get('self_healing_rate', 0):.1%}"
                                    noise_score = f"{monitor_quality.get('noise_score', 0):.1f}"

                        # Check removal candidates for confidence
                        if isinstance(removal_candidates, dict):
                            candidates_list = removal_candidates.get("items", [])
                            if isinstance(candidates_list, list):
                                for candidate in candidates_list:
                                    if isinstance(candidate, dict) and candidate.get("monitor_id") == monitor_id:
                                        confidence = f"{candidate.get('confidence_score', 0):.2f}"
                                        break

                        # Get individual monitor temporal data
                        ttr_value = stats.get("avg_time_to_resolution_minutes")
                        median_cycle = stats.get("median_cycle_duration_minutes")

                        ttr = f"{ttr_value:.1f}" if ttr_value is not None else "N/A"
                        median_cycle_str = f"{median_cycle:.1f}" if median_cycle is not None else "N/A"

                        business_hours_pct = stats.get("business_hours_percentage", 0)

                        # Extract service name from monitor name
                        service_name = "N/A"
                        if monitor_name and isinstance(monitor_name, str):
                            # Try to extract service from patterns like "[CORE SERVICES CATALOG] [TF] servicename-..."
                            import re
                            service_match = re.search(r'\[TF\]\s*([a-zA-Z0-9\-_]+)', monitor_name)
                            if service_match:
                                service_name = service_match.group(1)
                            elif "api" in monitor_name.lower():
                                # Fallback: try to extract service name containing 'api'
                                api_match = re.search(r'([a-zA-Z0-9\-_]*api[a-zA-Z0-9\-_]*)', monitor_name.lower())
                                if api_match:
                                    service_name = api_match.group(1)

                        # Create clickable link for monitor name with deletion check
                        is_deleted = stats.get("is_deleted", False)
                        monitor_link = EventsCommand._create_monitor_link(monitor_name, monitor_id, 50, is_deleted=is_deleted, deleted_monitors=deleted_monitors)

                        # Clean service name for markdown
                        clean_service = EventsCommand._clean_monitor_name_for_markdown(service_name, 20)

                        # Set status based on deletion
                        status = "🗑️ DELETED" if is_deleted else "✅ Active"

                        lines.append(f"| {monitor_link} | {clean_service} | `{monitor_id}` | {status} | {grade} | {health_score:.1f} | {total_events} | {events_per_day:.1f} | {cycles_per_week:.1f} | {self_heal_rate} | {noise_score} | {confidence} | {ttr} | {median_cycle_str} | {business_hours_pct:.1f}% |")

                    lines.append("")

                    # Add explanation of the table columns
                    lines.append("**Column Explanations:**")
                    lines.append("")
                    lines.append("- **Monitor Name**: Full monitor name (clickable link to Datadog monitor page)")
                    lines.append("- **Service**: Extracted service name from monitor configuration")
                    lines.append("- **Monitor ID**: Unique Datadog monitor identifier")
                    lines.append("- **Status**: Monitor status (✅ Active = exists in Datadog, 🗑️ DELETED = no longer exists)")
                    lines.append("- **Grade**: Overall business value grade (A=Excellent, B=Good, C=Fair, D=Poor, F=Critical)")
                    lines.append("- **Health Score**: Composite business value score 0-100 (higher is better)")
                    lines.append("- **Events**: Total number of alert events in the analysis period")
                    lines.append("- **Events/Day**: Average daily event volume")
                    lines.append("- **Cycles/Week**: Alert cycles per week (complete alert → recovery cycles)")
                    lines.append("- **Self-Heal Rate**: Percentage of alerts that resolve automatically")
                    lines.append("- **Noise Score**: Noisiness indicator 0-100 (higher = noisier)")
                    lines.append("- **Confidence**: Removal recommendation confidence score (if applicable)")
                    lines.append("- **TTR**: Individual monitor's Average Time To Resolution in minutes")
                    lines.append("- **Median Cycle**: Median duration of alert cycles for this monitor")
                    lines.append("- **Business Hours %**: Percentage of events during Brazilian business hours (9 AM - 5 PM BRT/BRST)")
                    lines.append("")
                    lines.append("💡 **Tip**: Click on any monitor name to open it directly in Datadog for detailed configuration and history.")
                    lines.append("")

                    # Add insights section
                    lines.append("**Key Insights from Individual Monitor Data:**")
                    lines.append("")

                    # Find monitors with different characteristics
                    high_volume = [item for item in monitor_items if item[1].get("total_events", 0) > 50]
                    low_grades = [item for item in monitor_items if item[1].get("health_score", {}).get("grade") in ["D", "F"]]
                    high_business_impact = [item for item in monitor_items if item[1].get("business_hours_percentage", 0) > 80]

                    # Grade distribution analysis
                    grade_counts = {}
                    for _, stats, _ in monitor_items:
                        grade = stats.get("health_score", {}).get("grade", "N/A")
                        grade_counts[grade] = grade_counts.get(grade, 0) + 1

                    lines.append("**Monitor Health Distribution:**")
                    total_monitors = len(monitor_items)
                    for grade in ["A", "B", "C", "D", "F"]:
                        count = grade_counts.get(grade, 0)
                        percentage = (count / total_monitors) * 100 if total_monitors > 0 else 0
                        if count > 0:
                            lines.append(f"- **Grade {grade}**: {count} monitors ({percentage:.1f}%)")
                    lines.append("")

                    # Volume analysis
                    if high_volume:
                        lines.append(f"**📊 High-Volume Monitors**: {len(high_volume)} monitors generate >50 events each")
                        top_volume = max(high_volume, key=lambda x: x[1].get("total_events", 0))
                        monitor_name = top_volume[1].get("monitor_name") or top_volume[0]
                        monitor_id = top_volume[0] if top_volume[0] != monitor_name else None
                        monitor_link = EventsCommand._create_monitor_link(monitor_name, monitor_id, 50, deleted_monitors=deleted_monitors)
                        event_count = top_volume[1].get("total_events", 0)
                        events_per_day = top_volume[1].get("events_per_day", 0)
                        lines.append(f"- *Highest Volume*: **{monitor_link}** with {event_count} events ({events_per_day:.1f}/day)")

                        # Calculate total events from high volume monitors
                        total_high_volume_events = sum(item[1].get("total_events", 0) for item in high_volume)
                        total_all_events = sum(item[1].get("total_events", 0) for item in monitor_items)
                        high_volume_percentage = (total_high_volume_events / max(total_all_events, 1)) * 100
                        lines.append(f"- These {len(high_volume)} monitors account for {high_volume_percentage:.1f}% of all events")
                        lines.append("")

                    # Poor health analysis
                    if low_grades:
                        lines.append(f"**🔴 Poor Health Monitors**: {len(low_grades)} monitors have grades D or F")
                        worst = low_grades[0]  # Already sorted by health score
                        monitor_name = worst[1].get("monitor_name") or worst[0]
                        monitor_id = worst[0] if worst[0] != monitor_name else None
                        monitor_link = EventsCommand._create_monitor_link(monitor_name, monitor_id, 50, deleted_monitors=deleted_monitors)
                        health_score = worst[2]
                        lines.append(f"- *Worst Health*: **{monitor_link}** with health score {health_score:.1f}/100")

                        # Show total events from poor health monitors
                        poor_health_events = sum(item[1].get("total_events", 0) for item in low_grades)
                        total_all_events = sum(item[1].get("total_events", 0) for item in monitor_items)
                        poor_health_percentage = (poor_health_events / max(total_all_events, 1)) * 100
                        lines.append(f"- Poor health monitors generate {poor_health_percentage:.1f}% of all events")
                        lines.append("")

                    # Business impact analysis
                    if high_business_impact:
                        lines.append(f"**⏰ Business-Critical Timing**: {len(high_business_impact)} monitors alert primarily during Brazilian business hours (>80%)")
                        lines.append("- These require immediate attention when they fire during working hours")

                        # Find the most business-critical
                        most_critical = max(high_business_impact, key=lambda x: x[1].get("business_hours_percentage", 0))
                        critical_name = most_critical[1].get("monitor_name") or most_critical[0]
                        monitor_id = most_critical[0] if most_critical[0] != critical_name else None
                        critical_link = EventsCommand._create_monitor_link(critical_name, monitor_id, 50, deleted_monitors=deleted_monitors)
                        critical_pct = most_critical[1].get("business_hours_percentage", 0)
                        lines.append(f"- *Most Business-Critical*: **{critical_link}** ({critical_pct:.1f}% business hours)")
                        lines.append("")

                    # TTR Analysis
                    monitors_with_ttr = [(item[0], item[1]) for item in monitor_items
                                       if item[1].get("avg_time_to_resolution_minutes") is not None]

                    if monitors_with_ttr:
                        ttrs = [stats.get("avg_time_to_resolution_minutes") for _, stats in monitors_with_ttr]
                        fastest_ttr = min(monitors_with_ttr, key=lambda x: x[1].get("avg_time_to_resolution_minutes", float('inf')))
                        slowest_ttr = max(monitors_with_ttr, key=lambda x: x[1].get("avg_time_to_resolution_minutes", 0))

                        lines.append("**⏱️ Resolution Time Analysis:**")
                        avg_ttr = sum(ttrs) / len(ttrs)
                        lines.append(f"- *Average TTR across all monitors*: {avg_ttr:.1f} minutes")

                        fastest_name = fastest_ttr[1].get("monitor_name") or fastest_ttr[0]
                        fastest_id = fastest_ttr[0] if fastest_ttr[0] != fastest_name else None
                        fastest_link = EventsCommand._create_monitor_link(fastest_name, fastest_id, 50, deleted_monitors=deleted_monitors)
                        fastest_time = fastest_ttr[1].get("avg_time_to_resolution_minutes", 0)
                        lines.append(f"- *Fastest Resolution*: **{fastest_link}** ({fastest_time:.1f} min)")

                        slowest_name = slowest_ttr[1].get("monitor_name") or slowest_ttr[0]
                        slowest_id = slowest_ttr[0] if slowest_ttr[0] != slowest_name else None
                        slowest_link = EventsCommand._create_monitor_link(slowest_name, slowest_id, 50, deleted_monitors=deleted_monitors)
                        slowest_time = slowest_ttr[1].get("avg_time_to_resolution_minutes", 0)
                        lines.append(f"- *Slowest Resolution*: **{slowest_link}** ({slowest_time:.1f} min)")
                        lines.append("")

                    # Self-healing analysis
                    lines.append("**🤖 Self-Healing Analysis:**")
                    excellent_healers = []
                    poor_healers = []

                    for monitor_id, stats, _ in monitor_items:
                        # Get self-healing data from quality metrics
                        if isinstance(alert_quality, dict):
                            quality_per_monitor = alert_quality.get("per_monitor", {})
                            if isinstance(quality_per_monitor, dict) and monitor_id in quality_per_monitor:
                                monitor_quality = quality_per_monitor[monitor_id]
                                if isinstance(monitor_quality, dict):
                                    self_heal = monitor_quality.get('self_healing_rate', 0)
                                    monitor_name = stats.get("monitor_name") or monitor_id
                                    monitor_link = EventsCommand._create_monitor_link(monitor_name, monitor_id, 40, deleted_monitors=deleted_monitors)
                                    if self_heal >= 0.8:
                                        excellent_healers.append((monitor_link, self_heal))
                                    elif self_heal <= 0.2:
                                        poor_healers.append((monitor_link, self_heal))

                    if excellent_healers:
                        lines.append(f"- **Excellent Self-Healers** (≥80%): {len(excellent_healers)} monitors")
                        best_healer = max(excellent_healers, key=lambda x: x[1])
                        lines.append(f"  - Best: *{best_healer[0]}* ({best_healer[1]:.1%} self-healing)")

                    if poor_healers:
                        lines.append(f"- **Poor Self-Healers** (≤20%): {len(poor_healers)} monitors")
                        worst_healer = min(poor_healers, key=lambda x: x[1])
                        lines.append(f"  - Worst: *{worst_healer[0]}* ({worst_healer[1]:.1%} self-healing)")

                    lines.append("")

                # Add methodology explanation
                lines.append("##### Analysis Methodology")
                lines.append("")
                lines.append("**Business Value Score Calculation (0-100):**")
                lines.append("")
                lines.append("*Our scoring prioritizes alerts that provide real business value over noisy, self-resolving alerts.*")
                lines.append("")
                lines.append("- **Alert Relevance (35%)**: Does this alert represent a real problem?")
                lines.append("  - *Penalizes alerts that resolve too quickly (< 5min) as likely noise*")
                lines.append("  - *Rewards alerts that give time to investigate and act*")
                lines.append("- **Response Necessity (30%)**: Does this alert require human action?")
                lines.append("  - *High scores for alerts that consistently need intervention*")
                lines.append("  - *Penalties for excessive self-healing (> 80% quick recovery)*")
                lines.append("- **Business Impact (20%)**: Does this alert affect business operations?")
                lines.append("  - *Factors in event volume and potential service impact*")
                lines.append("  - *Higher scores for alerts from critical services*")
                lines.append("- **Timing Quality (15%)**: Does this alert fire at the right time?")
                lines.append("  - *Optimal duration: 5-60 minutes (time to investigate)*")
                lines.append("  - *Penalties for incomplete alert cycles*")
                lines.append("")
                lines.append("**Key Insight**: *Self-healing is GOOD only when it gives teams time to act. Quick self-healing (< 5min) usually indicates noise.*")
                lines.append("")
                lines.append("**Business Impact Factors:**")
                lines.append("- Business hours events (9 AM - 5 PM Brazilian time)")
                lines.append("- Weekend alerting patterns (Brazilian timezone)")
                lines.append("- High-priority event distribution")
                lines.append("- Estimated manual response requirements")
                lines.append("")



        # Add monitors analysis section if available (moved before Team Alerts)
        monitors_data = payload.get("monitors", {}) if isinstance(payload, dict) else {}
        if isinstance(monitors_data, dict) and monitors_data:
            lines.append("")
            lines.append("### 📊 Monitors Analysis")
            lines.append("")

            summary = monitors_data.get("summary", {})
            unused_monitors = monitors_data.get("unused_monitors", [])
            active_monitors = monitors_data.get("active_monitors", [])

            # Summary stats
            total_monitors = summary.get("total_monitors", 0)
            unused_count = summary.get("unused_monitors", 0)
            active_count = summary.get("active_monitors", 0)
            analysis_days = summary.get("analysis_period_days", 0)

            lines.append(f"**Analysis Period**: Last {analysis_days} days  ")
            lines.append(f"**Total Monitors Found**: {total_monitors}  ")
            lines.append(f"**Active Monitors**: {active_count}  ")
            lines.append(f"**Unused Monitors**: {unused_count}  ")
            lines.append(f"**Muted Monitors**: {summary.get('muted_monitors', 0)}  ")
            lines.append("")

            # Group monitors by team for organization
            monitors_by_team = {}

            # Process unused monitors
            for monitor in unused_monitors:
                team_tags = [tag.split("team:", 1)[1] for tag in monitor.get("tags", []) if tag.startswith("team:")]
                for team in team_tags:
                    if team not in monitors_by_team:
                        monitors_by_team[team] = {"active": [], "unused": []}
                    monitors_by_team[team]["unused"].append(monitor)

            # Process active monitors (if included)
            if active_monitors:
                for monitor in active_monitors:
                    team_tags = [tag.split("team:", 1)[1] for tag in monitor.get("tags", []) if tag.startswith("team:")]
                    for team in team_tags:
                        if team not in monitors_by_team:
                            monitors_by_team[team] = {"active": [], "unused": []}
                        monitors_by_team[team]["active"].append(monitor)

            # Create table for unused monitors by team
            if unused_count > 0:
                lines.append("#### 📵 Unused Monitors by Team")
                lines.append("")
                lines.append("*Monitors that haven't triggered events in the analysis period:*")
                lines.append("")

                # Get DD_SITE for links
                dd_site = os.getenv("DD_SITE", "datadoghq.eu")

                for team in sorted(monitors_by_team.keys()):
                    team_data = monitors_by_team[team]
                    team_unused = len(team_data["unused"])

                    if team_unused > 0:
                        lines.append(f"**Team: {team}** ({team_unused} unused monitors)")
                        lines.append("")
                        lines.append("| Monitor | Type | Status | Last Triggered | Muted |")
                        lines.append("|---------|------|--------|----------------|-------|")

                        for monitor in team_data["unused"][:20]:  # Show up to 20 per team
                            name = EventsCommand._clean_monitor_name_for_markdown(monitor.get("name", "Unknown"), 40)
                            monitor_id = monitor.get("id", "N/A")
                            monitor_url = f"https://app.{dd_site}/monitors/{monitor_id}"

                            days_ago = monitor.get("last_triggered_days_ago")
                            days_text = f"{days_ago}d ago" if days_ago else "Never"

                            status_icon = "🔇" if monitor.get("muted") else ""
                            muted_text = "Yes" if monitor.get("muted") else "No"

                            lines.append(f"| [{name}]({monitor_url}) | {monitor.get('classification', 'N/A')} | {monitor.get('status', 'N/A')} {status_icon} | {days_text} | {muted_text} |")

                        if team_unused > 20:
                            lines.append(f"| ... and {team_unused - 20} more monitors | | | | |")

                        lines.append("")

            # Active monitors summary (if included)
            if active_monitors:
                lines.append("#### ✅ Active Monitors Summary")
                lines.append("")
                lines.append("*Recent activity distribution:*")
                lines.append("")

                # Group by recent activity
                recent_activity = {}
                for monitor in active_monitors:
                    days_ago = monitor.get("last_triggered_days_ago", 999)
                    if days_ago <= 1:
                        bucket = "Last 24h"
                    elif days_ago <= 7:
                        bucket = "Last 7 days"
                    elif days_ago <= 30:
                        bucket = "Last 30 days"
                    else:
                        bucket = "30+ days"

                    recent_activity[bucket] = recent_activity.get(bucket, 0) + 1

                lines.append("| Activity Period | Count |")
                lines.append("|-----------------|-------|")
                for period in ["Last 24h", "Last 7 days", "Last 30 days", "30+ days"]:
                    count = recent_activity.get(period, 0)
                    if count > 0:
                        lines.append(f"| {period} | {count} |")
                lines.append("")

            # Insights and recommendations
            if unused_count == 0:
                lines.append("🎉 **Excellent!** All monitors have been active in the analysis period.")
            else:
                lines.append(f"💡 **Recommendation**: Review the {unused_count} unused monitors for potential:")
                lines.append("")
                lines.append("- **Removal**: If no longer needed")
                lines.append("- **Threshold adjustment**: If too sensitive")
                lines.append("- **Scope revision**: If monitoring wrong metrics")
                lines.append("- **Environment verification**: If environment tags are incorrect")

            lines.append("")

        # Add Team Alerts section only if requested
        if getattr(args, 'show_team_alerts', False):
            lines.append("### Team Alerts")
            lines.append("")
            if not teams:
                lines.append("No teams returned events for the selected period.")
            else:
                for team, data in teams.items():
                    if not isinstance(data, dict):
                        continue
                    lines.append(f"#### Team: {team}")
                    lines.append("")
                    if data.get("error"):
                        lines.append(f"- ⚠️ Error: {data['error']}")
                        lines.append("")
                        continue
                    lines.append(f"- Events: {data.get('event_count', 0)}")
                    last_event = data.get("last_event_timestamp")
                    if last_event:
                        lines.append(f"- Last Event: {last_event}")
                    lines.append("")
                    events = data.get("events", [])
                    if not events:
                        lines.append("No monitor alerts for this team within the selected window.")
                        lines.append("")
                        continue
                    lines.append("| Timestamp | Alert Type | Title | Monitor | Source |")
                    lines.append("|---|---|---|---|---|")
                    for event in events:
                        title = (event.get("title") or "").replace("|", "\\|")
                        alert_type = event.get("alert_type") or "alert"
                        monitor = event.get("monitor") or {}
                        monitor_name = monitor.get("name") or "Unknown"
                        monitor_id = monitor.get("id")
                        monitor_label = EventsCommand._create_monitor_link(monitor_name, monitor_id, 30, deleted_monitors=deleted_monitors) if monitor_name != "Unknown" else "-"
                        timestamp = event.get("timestamp") or "-"
                        source = event.get("source") or "-"
                        lines.append(
                            f"| {timestamp} | {alert_type} | {title} | {monitor_label} | {source} |"
                        )
                    lines.append("")

        # Add footer with report information at the end
        if isinstance(advanced_analysis, dict) and advanced_analysis:
            lines.append("")
            lines.append("---")
            lines.append("")
            lines.append("### Report Information")
            lines.append("")
            lines.append("**About This Analysis:**")
            lines.append("")
            lines.append("This report uses advanced data analysis to identify monitoring optimization opportunities:")
            lines.append("")
            lines.append("- **Alert Quality Metrics**: Composite scores based on self-healing rates, noise patterns, and operational efficiency")
            lines.append("- **Removal Candidates**: Data-driven recommendations using machine learning-like scoring algorithms")
            lines.append("- **Health Scores**: Multi-factor analysis considering business impact, temporal patterns, and response requirements")
            lines.append("- **Actionability Analysis**: Prioritization based on confidence levels and potential noise reduction")
            lines.append("")
            lines.append("**How to Use These Insights:**")
            lines.append("")
            lines.append("1. 🔴 **Start with high-confidence removal candidates** (confidence > 0.85)")
            lines.append("2. 🤖 **Consider automation** for monitors with >70% quick recovery rates")
            lines.append("3. ⚙️ **Adjust thresholds** for monitors with poor health scores but high business value")
            lines.append("4. 📊 **Monitor trends over time** by running this analysis regularly")
            lines.append("")
            lines.append("**Safety Guidelines:**")
            lines.append("")
            lines.append("- Always review recommendations with domain experts before acting")
            lines.append("- Test threshold adjustments in non-production environments first")
            lines.append("- Keep backup configurations before making monitor changes")
            lines.append("- Monitor the impact of changes for at least one week")
            lines.append("")
            lines.append("*Report generated by PyToolkit Advanced Datadog Events Analysis*")

        return "\n".join(lines)
