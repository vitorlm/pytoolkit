"""
Cycle Time Analysis Results Formatter

This module handles all formatting and display logic for cycle time analysis results.
Separates presentation logic from business logic following clean architecture principles.
"""

from typing import List

from domains.syngenta.jira.sle_config import SLEConfig


class CycleTimeFormatter:
    """Handles formatting and display of cycle time analysis results."""

    def __init__(self):
        """Initialize formatter with SLE configuration."""
        self.sle_config = SLEConfig()

    def display_enhanced_console(self, results: dict) -> None:
        """
        Display enhanced console output with emojis and performance indicators.

        Args:
            results (dict): Analysis results from CycleTimeService
        """
        metadata = results.get("analysis_metadata", {})
        metrics = results.get("metrics", {})

        self._print_header(metadata, metrics)
        self._print_executive_summary(metrics)
        self._print_metrics(metrics)
        self._print_sle_targets()
        self._print_sle_compliance(results)
        self._print_time_distribution(metrics)
        self._print_priority_breakdown(metrics)

        # Print trending analysis if available
        trending_analysis = results.get("trending_analysis")
        if trending_analysis:
            self._print_trending_analysis(trending_analysis)

        self._print_performance_analysis(metrics)
        self._print_sample_issues(results)
        self._print_footer()

    def _print_header(self, metadata: dict, metrics: dict) -> None:
        """Print report header with metadata."""
        project_key = metadata.get("project_key", "Unknown")
        time_period = metadata.get("time_period", "Unknown")
        start_date = metadata.get("start_date", "")
        end_date = metadata.get("end_date", "")
        issue_types = metadata.get("issue_types", [])
        team = metadata.get("team")
        priorities = metadata.get("priorities", [])

        # Calculate performance emoji
        avg_cycle_time = metrics.get("average_cycle_time_hours", 0)
        perf_emoji = self._get_performance_emoji(avg_cycle_time)

        print(f"\n{perf_emoji} CYCLE TIME ANALYSIS REPORT")
        print("=" * 80)
        print(f"📊 Project: {project_key}")
        print(f"📅 Period: {time_period}")
        if start_date and end_date:
            print(f"📆 Range: {start_date[:10]} to {end_date[:10]}")
        print(f"🎯 Issue Types: {', '.join(issue_types)}")
        if team:
            print(f"👥 Team: {team}")
        if priorities:
            print(f"⚡ Priorities: {', '.join(priorities)}")
        print("=" * 80)

    def _print_executive_summary(self, metrics: dict) -> None:
        """Print executive summary section."""
        avg_cycle_time = metrics.get("average_cycle_time_hours", 0)
        avg_days = avg_cycle_time / 24
        avg_lead_time = metrics.get("average_lead_time_hours", 0)
        avg_lead_days = avg_lead_time / 24

        perf_emoji = self._get_performance_emoji(avg_cycle_time)
        performance = self._get_performance_text(avg_cycle_time)

        print(f"\n{perf_emoji} EXECUTIVE SUMMARY")
        print("-" * 40)
        print(f"⏱️  Average Cycle Time: {avg_cycle_time:.1f} hours ({avg_days:.1f} days)")
        print(f"🔄 Average Lead Time: {avg_lead_time:.1f} hours ({avg_lead_days:.1f} days)")
        print(f"📈 Performance: {performance}")
        print(f"📋 Total Issues: {metrics.get('total_issues', 0)}")
        print(f"✅ With Valid Cycle Time: {metrics.get('issues_with_valid_cycle_time', 0)}")

        # Show anomaly information
        anomaly_count = metrics.get("zero_cycle_time_anomalies", 0)
        anomaly_percentage = metrics.get("anomaly_percentage", 0)
        if anomaly_count > 0:
            print(f"🚨 Zero Cycle Time Anomalies: {anomaly_count} ({anomaly_percentage:.1f}%)")

    def _print_metrics(self, metrics: dict) -> None:
        """Print cycle time and lead time metrics."""
        # Cycle time metrics
        median_cycle_time = metrics.get("median_cycle_time_hours", 0)
        median_days = median_cycle_time / 24
        min_cycle_time = metrics.get("min_cycle_time_hours", 0)
        min_days = min_cycle_time / 24
        max_cycle_time = metrics.get("max_cycle_time_hours", 0)
        max_days = max_cycle_time / 24

        # Lead time metrics
        median_lead_time = metrics.get("median_lead_time_hours", 0)
        median_lead_days = median_lead_time / 24
        min_lead_time = metrics.get("min_lead_time_hours", 0)
        min_lead_days = min_lead_time / 24
        max_lead_time = metrics.get("max_lead_time_hours", 0)
        max_lead_days = max_lead_time / 24

        print("\n📊 CYCLE TIME METRICS:")
        print(f"🎯 Median: {median_cycle_time:.1f}h ({median_days:.1f}d)")
        print(f"🚀 Fastest: {min_cycle_time:.1f}h ({min_days:.1f}d)")
        print(f"🐌 Slowest: {max_cycle_time:.1f}h ({max_days:.1f}d)")

        print("\n🔄 LEAD TIME METRICS:")
        print(f"🎯 Median: {median_lead_time:.1f}h ({median_lead_days:.1f}d)")
        print(f"🚀 Fastest: {min_lead_time:.1f}h ({min_lead_days:.1f}d)")
        print(f"🐌 Slowest: {max_lead_time:.1f}h ({max_lead_days:.1f}d)")

    def _print_sle_targets(self) -> None:
        """Print SLE targets information."""
        print("\n📋 SERVICE LEVEL EXPECTATIONS (SLE)")
        print("-" * 40)
        sle_targets = self.sle_config.get_all_targets()
        for priority, target_hours in sle_targets.items():
            target_days = target_hours / 24
            print(f"🎯 {priority}: {target_hours:.0f}h ({target_days:.1f}d)")

    def _print_sle_compliance(self, results: dict) -> None:
        """Print SLE compliance analysis."""
        print("\n📈 SLE COMPLIANCE ANALYSIS")
        print("-" * 40)

        issues = results.get("issues", [])
        anomalies = results.get("anomalies", [])
        total_issues_analyzed = len(issues) + len(anomalies)
        sle_compliant_cycle = 0
        sle_compliant_lead = 0

        # Check compliance for valid cycle time issues
        for issue in issues:
            if issue.get("has_valid_cycle_time", False):
                priority = issue.get("priority")
                cycle_time = issue.get("cycle_time_hours", 0)
                lead_time = issue.get("lead_time_hours", 0)

                cycle_compliance = self.sle_config.check_sle_compliance(priority, cycle_time)
                lead_compliance = self.sle_config.check_sle_compliance(priority, lead_time)

                if cycle_compliance["is_compliant"]:
                    sle_compliant_cycle += 1
                if lead_compliance["is_compliant"]:
                    sle_compliant_lead += 1

        # Check compliance for anomalies (using lead time)
        for anomaly in anomalies:
            priority = anomaly.get("priority")
            lead_time = anomaly.get("lead_time_hours", 0)

            lead_compliance = self.sle_config.check_sle_compliance(priority, lead_time)
            if lead_compliance["is_compliant"]:
                sle_compliant_lead += 1

        if total_issues_analyzed > 0:
            cycle_compliance_rate = (sle_compliant_cycle / len(issues)) * 100 if issues else 0
            lead_compliance_rate = (sle_compliant_lead / total_issues_analyzed) * 100

            cycle_emoji = "✅" if cycle_compliance_rate >= 80 else "⚠️" if cycle_compliance_rate >= 60 else "❌"
            lead_emoji = "✅" if lead_compliance_rate >= 80 else "⚠️" if lead_compliance_rate >= 60 else "❌"

            print(
                f"{cycle_emoji} Cycle Time SLE Compliance: {sle_compliant_cycle}/{len(issues)} ({cycle_compliance_rate:.1f}%)"
            )
            print(
                f"{lead_emoji} Lead Time SLE Compliance: {sle_compliant_lead}/{total_issues_analyzed} ({lead_compliance_rate:.1f}%)"
            )

    def _print_time_distribution(self, metrics: dict) -> None:
        """Print time distribution analysis."""
        time_distribution = metrics.get("time_distribution", {})
        if time_distribution:
            print("\n📊 TIME DISTRIBUTION")
            print("-" * 40)
            total_with_cycle_time = metrics.get("issues_with_valid_cycle_time", 1)

            for time_range, count in time_distribution.items():
                if count > 0:
                    percentage = (count / total_with_cycle_time) * 100

                    # Determine emoji based on time range
                    if "< 4 hours" in time_range or "4-8 hours" in time_range:
                        emoji = "🟢"
                    elif "8-24 hours" in time_range or "1-3 days" in time_range:
                        emoji = "🟡"
                    elif "3-7 days" in time_range:
                        emoji = "🟠"
                    else:
                        emoji = "🔴"

                    print(f"{emoji} {time_range}: {count} issues ({percentage:.1f}%)")

    def _print_priority_breakdown(self, metrics: dict) -> None:
        """Print priority breakdown analysis."""
        priority_breakdown = metrics.get("priority_breakdown", {})
        if priority_breakdown:
            print("\n🎯 PRIORITY ANALYSIS & SLE COMPLIANCE")
            print("-" * 40)

            # Sort by average cycle time
            sorted_priorities = sorted(
                priority_breakdown.items(), key=lambda x: x[1].get("average_cycle_time_hours", 0)
            )

            for priority, p_metrics in sorted_priorities:
                count = p_metrics.get("count", 0)
                avg_hours = p_metrics.get("average_cycle_time_hours", 0)
                avg_days = avg_hours / 24

                # Lead time metrics for this priority
                avg_lead_hours = p_metrics.get("average_lead_time_hours", 0)
                avg_lead_days = avg_lead_hours / 24

                # Priority emoji
                priority_emoji = self._get_priority_emoji(priority)

                # SLE compliance check for both cycle time and lead time
                cycle_sle_compliance = self.sle_config.check_sle_compliance(priority, avg_hours)
                lead_sle_compliance = self.sle_config.check_sle_compliance(priority, avg_lead_hours)
                cycle_sle_emoji = self.sle_config.get_sle_emoji(priority, avg_hours)
                lead_sle_emoji = self.sle_config.get_sle_emoji(priority, avg_lead_hours)
                sle_target_hours = cycle_sle_compliance["target_hours"]
                sle_target_days = cycle_sle_compliance["target_days"]

                print(f"{priority_emoji} {priority}: {count} issues")
                print(f"    Cycle Time: {avg_hours:.1f}h ({avg_days:.1f}d) {cycle_sle_emoji}")
                print(f"    Lead Time: {avg_lead_hours:.1f}h ({avg_lead_days:.1f}d) {lead_sle_emoji}")
                print(f"    SLE Target: {sle_target_hours:.0f}h ({sle_target_days:.1f}d)")
                print(f"    Cycle: {cycle_sle_compliance['status']} | Lead: {lead_sle_compliance['status']}")
                print()

    def _print_performance_analysis(self, metrics: dict) -> None:
        """Print performance analysis and recommendations."""
        print("\n📈 PERFORMANCE ANALYSIS")
        print("-" * 40)

        avg_cycle_time = metrics.get("average_cycle_time_hours", 0)

        if avg_cycle_time < 24:
            print("✅ Excellent performance! Fast resolution times indicate efficient processes.")
            print("💡 Continue current practices and share success patterns with other teams.")
        elif avg_cycle_time < 72:
            print("🟡 Good performance with opportunities for optimization.")
            print("💡 Analyze top performers and eliminate minor bottlenecks.")
        elif avg_cycle_time < 168:
            print("🟠 Moderate performance - process improvements needed.")
            print("💡 Focus on identifying delays and optimizing workflow.")
        else:
            print("🔴 Performance concerns - immediate attention required.")
            print("💡 Review processes and consider resource reallocation.")

    def _print_sample_issues(self, results: dict) -> None:
        """Print sample issues for analysis."""
        issues = results.get("issues", [])
        anomalies = results.get("anomalies", [])

        # Show anomalies first if any exist
        if anomalies:
            self._print_anomalies(anomalies)

        # Show valid issues
        if issues:
            valid_issues = [issue for issue in issues if issue.get("has_valid_cycle_time", False)]
            if valid_issues:
                self._print_cycle_time_analysis(valid_issues)
                self._print_lead_time_analysis(valid_issues)

    def _print_anomalies(self, anomalies: List[dict]) -> None:
        """Print anomaly details."""
        print("\n🚨 ZERO CYCLE TIME ANOMALIES")
        print("-" * 40)
        print(f"Found {len(anomalies)} issues with 0.0h cycle time (batch updates/admin closures)")
        print("Using lead time as alternative metric:")
        print("\n📋 ANOMALY DETAILS (showing Lead Time, not Cycle Time):")

        # Sort anomalies by lead time
        sorted_anomalies = sorted(anomalies, key=lambda x: x.get("lead_time_hours", 0))

        for i, issue in enumerate(sorted_anomalies[:6], 1):
            issue_key = issue.get("issue_key", "N/A")
            lead_time_hours = issue.get("lead_time_hours", 0)
            lead_time_days = issue.get("lead_time_days", 0)
            priority = issue.get("priority")
            summary = issue.get("summary", "N/A")
            batch_indicator = " 🔄" if issue.get("has_batch_update_pattern", False) else " 📋"
            priority_prefix = self._get_priority_prefix(priority or "Unknown")
            if len(summary) > 60:
                summary = summary[:57] + "..."
            print(
                f"  {i}. {priority_prefix}[{issue_key}] {summary}: {lead_time_hours:.1f}h ({lead_time_days:.1f}d){batch_indicator}"
            )

        if len(sorted_anomalies) > 6:
            print(f"  ... and {len(sorted_anomalies) - 6} more anomalies")

    def _print_cycle_time_analysis(self, valid_issues: List[dict]) -> None:
        """Print cycle time analysis section."""
        print("\n📝 CYCLE TIME ANALYSIS")
        print("-" * 40)

        # Sort by cycle time
        sorted_issues = sorted(valid_issues, key=lambda x: x.get("cycle_time_hours", 0))

        print("🚀 Fastest 3 Issues (Cycle Time):")
        for i, issue in enumerate(sorted_issues[:3], 1):
            self._print_issue_line(issue, "cycle_time", i)

        if len(sorted_issues) > 3:
            print("\n🐌 Slowest 3 Issues (Cycle Time):")
            for i, issue in enumerate(sorted_issues[-3:], 1):
                self._print_issue_line(issue, "cycle_time", i)

    def _print_lead_time_analysis(self, valid_issues: List[dict]) -> None:
        """Print lead time analysis section."""
        print("\n📊 LEAD TIME ANALYSIS")
        print("-" * 40)

        # Sort by lead time
        sorted_by_lead_time = sorted(valid_issues, key=lambda x: x.get("lead_time_hours", 0))

        print("⚡ Fastest 3 Issues (Lead Time):")
        for i, issue in enumerate(sorted_by_lead_time[:3], 1):
            self._print_issue_line(issue, "lead_time", i)

        if len(sorted_by_lead_time) > 3:
            print("\n🕐 Slowest 3 Issues (Lead Time):")
            for i, issue in enumerate(sorted_by_lead_time[-3:], 1):
                self._print_issue_line(issue, "lead_time", i)

    def _print_issue_line(self, issue: dict, time_type: str, index: int) -> None:
        """Print a single issue line with formatting."""
        issue_key = issue.get("issue_key", "N/A")
        priority = issue.get("priority")
        summary = issue.get("summary", "N/A")

        if time_type == "cycle_time":
            time_hours = issue.get("cycle_time_hours", 0)
            time_days = issue.get("cycle_time_days", 0)
        else:  # lead_time
            time_hours = issue.get("lead_time_hours", 0)
            time_days = issue.get("lead_time_days", 0)

        sle_emoji = self.sle_config.get_sle_emoji(priority, time_hours)
        priority_prefix = self._get_priority_prefix(priority or "Unknown")

        if len(summary) > 60:
            summary = summary[:57] + "..."

        print(f"  {index}. {priority_prefix}[{issue_key}] {summary}: {time_hours:.1f}h ({time_days:.1f}d) {sle_emoji}")

    def _print_footer(self) -> None:
        """Print report footer."""
        print("\n" + "=" * 80)
        print("💡 Use --output-format md for detailed markdown report")
        print("💡 Use --output-format json for machine-readable data")
        print("💡 Use --enable-trending for trend analysis and alerts")
        print("=" * 80)

    def _print_trending_analysis(self, trending_analysis: dict) -> None:
        """Print trending analysis section."""
        print("\n📈 TRENDING ANALYSIS")
        print("=" * 80)

        trend_metrics = trending_analysis.get("trend_metrics", [])
        alerts = trending_analysis.get("alerts", [])
        baseline_period = trending_analysis.get("baseline_period", {})

        # Print baseline period info
        if baseline_period:
            baseline_start = baseline_period.get("start", "")[:10]  # YYYY-MM-DD
            baseline_end = baseline_period.get("end", "")[:10]
            print(f"📊 Baseline Period: {baseline_start} to {baseline_end}")
            print("-" * 40)

        # Print trend metrics
        if trend_metrics:
            print("\n🔄 TREND METRICS:")
            print("-" * 40)

            for trend in trend_metrics:
                metric_name = trend.metric_name
                current_value = trend.current_value
                baseline_value = trend.baseline_value
                change_percent = trend.change_percent
                trend_direction = trend.trend_direction
                significance = trend.significance

                # Get appropriate emojis
                direction_emoji = self._get_trend_direction_emoji(trend_direction, metric_name)
                significance_emoji = "📊" if significance else "📋"

                # Format values based on metric type
                if "Time" in metric_name:
                    current_str = f"{current_value:.1f}h"
                    baseline_str = f"{baseline_value:.1f}h"
                elif "Rate" in metric_name or "Compliance" in metric_name:
                    current_str = f"{current_value:.1f}%"
                    baseline_str = f"{baseline_value:.1f}%"
                else:
                    current_str = f"{current_value:.0f}"
                    baseline_str = f"{baseline_value:.0f}"

                change_str = f"{change_percent:+.1f}%" if change_percent != 0 else "0.0%"

                print(f"  {direction_emoji} {metric_name}:")
                print(
                    f"    Current: {current_str} | Baseline: {baseline_str} | Change: {change_str} {significance_emoji}"
                )

        # Print alerts
        if alerts:
            self._print_trend_alerts(alerts)
        else:
            print("\n✅ No alerts detected - all metrics within normal ranges")

    def _print_trend_alerts(self, alerts: List) -> None:
        """Print trend alerts section."""
        print(f"\n🚨 TREND ALERTS ({len(alerts)} total)")
        print("-" * 40)

        # Group alerts by severity
        critical_alerts = [a for a in alerts if a.severity == "CRITICAL"]
        warning_alerts = [a for a in alerts if a.severity == "WARNING"]
        info_alerts = [a for a in alerts if a.severity == "INFO"]

        # Print critical alerts first
        if critical_alerts:
            print("\n🔴 CRITICAL ALERTS:")
            for alert in critical_alerts:
                self._print_single_alert(alert)

        # Print warning alerts
        if warning_alerts:
            print("\n🟡 WARNING ALERTS:")
            for alert in warning_alerts:
                self._print_single_alert(alert)

        # Print info alerts
        if info_alerts:
            print("\n🔵 INFO ALERTS:")
            for alert in info_alerts:
                self._print_single_alert(alert)

    def _print_single_alert(self, alert) -> None:
        """Print a single alert."""
        message = alert.message
        recommendation = alert.recommendation
        current_value = alert.current_value
        threshold = alert.threshold

        print(f"  • {message}")
        print(f"    📊 Current: {current_value:.1f} | Threshold: {threshold:.1f}")
        print(f"    💡 {recommendation}")
        print()

    @staticmethod
    def _get_trend_direction_emoji(direction: str, metric_name: str) -> str:
        """Get emoji for trend direction based on metric type."""
        if direction == "STABLE":
            return "➡️"

        # For metrics where lower is better (cycle time, anomaly rate)
        lower_is_better = any(keyword in metric_name.lower() for keyword in ["time", "anomaly"])

        if direction == "IMPROVING":
            return "📈" if not lower_is_better else "📉"
        elif direction == "DEGRADING":
            return "📉" if not lower_is_better else "📈"
        else:
            return "❓"

    @staticmethod
    def _get_priority_prefix(priority: str) -> str:
        """
        Convert priority name to priority number for display.

        Args:
            priority: Priority name (e.g., "Critical [P1]", "High [P2]", etc.)

        Returns:
            Priority prefix in format [PZ] where Z is 1-4
        """
        if not priority:
            return "[P?]"

        import re

        # Try to extract [PZ] pattern from the priority string
        # Handles cases like "Critical [P1]", "High [P2]", etc.
        match = re.search(r"\[P(\d)\]", priority)
        if match:
            priority_number = match.group(1)
            return f"[P{priority_number}]"

        # Fallback: try to extract just the number after P
        match = re.search(r"P(\d)", priority)
        if match:
            priority_number = match.group(1)
            return f"[P{priority_number}]"

        # If no pattern found, try traditional mapping as fallback
        priority_normalized = priority.strip().lower()
        priority_map = {
            "critical": "[P1]",
            "highest": "[P1]",
            "blocker": "[P1]",
            "high": "[P2]",
            "major": "[P2]",
            "medium": "[P3]",
            "normal": "[P3]",
            "low": "[P4]",
            "minor": "[P4]",
            "lowest": "[P4]",
            "trivial": "[P4]",
        }
        mapped_priority = priority_map.get(priority_normalized)
        if mapped_priority:
            return mapped_priority

        # Debug: show what we couldn't parse
        return f"[P?:{priority[:10]}]"

    @staticmethod
    def _get_performance_emoji(avg_cycle_time: float) -> str:
        """Get performance emoji based on average cycle time."""
        if avg_cycle_time < 8:
            return "🟢"
        elif avg_cycle_time < 24:
            return "🟡"
        elif avg_cycle_time < 72:
            return "🟠"
        elif avg_cycle_time < 168:
            return "🔴"
        else:
            return "⛔"

    @staticmethod
    def _get_performance_text(avg_cycle_time: float) -> str:
        """Get performance text based on average cycle time."""
        if avg_cycle_time < 8:
            return "Excellent"
        elif avg_cycle_time < 24:
            return "Good"
        elif avg_cycle_time < 72:
            return "Moderate"
        elif avg_cycle_time < 168:
            return "Concerning"
        else:
            return "Poor"

    @staticmethod
    def _get_priority_emoji(priority: str) -> str:
        """Get emoji for priority level."""
        if "critical" in priority.lower() or "[p1]" in priority.lower():
            return "🔥"
        elif "high" in priority.lower() or "[p2]" in priority.lower():
            return "⚡"
        elif "medium" in priority.lower() or "[p3]" in priority.lower():
            return "🟡"
        elif "low" in priority.lower() or "[p4]" in priority.lower():
            return "🟢"
        else:
            return "⚪"
