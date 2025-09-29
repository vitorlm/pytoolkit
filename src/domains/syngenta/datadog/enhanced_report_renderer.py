"""Enhanced report renderer with auto-healing classification and trend analysis sections."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Dict, Any, List, Optional

from .enhanced_alert_cycle import CycleClassification
from .trend_analyzer import TrendDirection


class EnhancedReportRenderer:
    """Renderer for enhanced Datadog analysis reports."""

    def __init__(self, datadog_site: str = "datadoghq.eu"):
        self.datadog_site = datadog_site
        self.datadog_base_url = f"https://app.{datadog_site}"

    def render_enhanced_analysis_section(
        self,
        enhanced_quality: Dict[str, Any],
        recommendations: Dict[str, Any]
    ) -> List[str]:
        """Render enhanced analysis section with classification results."""
        lines = []

        lines.append("### 🔬 Enhanced Auto-Healing Analysis")
        lines.append("")
        lines.append("**Advanced classification distinguishes between flapping, benign transients, and actionable alerts:**")
        lines.append("")

        # Overall classification summary
        classification_summary = enhanced_quality.get("classification_summary", {})
        if classification_summary:
            total = classification_summary.get("total_cycles", 0)
            flapping = classification_summary.get("flapping_cycles", 0)
            benign = classification_summary.get("benign_transient_cycles", 0)
            actionable = classification_summary.get("actionable_cycles", 0)

            lines.append("#### Alert Cycle Classification")
            lines.append("")
            lines.append(f"- **Total Cycles Analyzed**: {total}")
            lines.append(f"- **🔄 Flapping Cycles**: {flapping} ({flapping/max(total,1)*100:.1f}%)")
            lines.append("  - *Rapid state oscillations indicating threshold or system issues*")
            lines.append(f"- **⚡ Benign Transient Cycles**: {benign} ({benign/max(total,1)*100:.1f}%)")
            lines.append("  - *Short-lived, self-resolving issues requiring no human action*")
            lines.append(f"- **🎯 Actionable Cycles**: {actionable} ({actionable/max(total,1)*100:.1f}%)")
            lines.append("  - *Legitimate alerts requiring or benefiting from human intervention*")
            lines.append("")

            # Confidence in classifications
            avg_confidence = classification_summary.get("avg_confidence", 0.0)
            lines.append(f"- **Average Classification Confidence**: {avg_confidence:.1%}")
            if avg_confidence >= 0.8:
                lines.append("  - 🟢 *High confidence - classifications are reliable*")
            elif avg_confidence >= 0.6:
                lines.append("  - 🟡 *Medium confidence - review edge cases*")
            else:
                lines.append("  - 🔴 *Low confidence - may need threshold tuning*")
            lines.append("")

        # Recommendations by type
        self._add_recommendations_section(lines, recommendations)

        return lines

    def _add_recommendations_section(
        self,
        lines: List[str],
        recommendations: Dict[str, Any]
    ) -> None:
        """Add recommendations sections."""
        lines.append("#### 🎯 Actionable Recommendations")
        lines.append("")

        # Flapping mitigation
        flapping_recs = recommendations.get("threshold_adjustments", [])
        if flapping_recs:
            lines.append("##### 🔄 Flapping Mitigation")
            lines.append("")
            lines.append("**Monitors showing excessive state oscillations:**")
            lines.append("")
            for rec in flapping_recs[:5]:  # Top 5
                monitor_name = rec.get("monitor_name", "Unknown")
                monitor_id = rec.get("monitor_id")
                action = rec.get("action", "")
                reason = rec.get("reason", "")

                monitor_link = self._create_monitor_link(monitor_name, monitor_id)

                lines.append(f"- **{monitor_link}**")
                lines.append(f"  - *Issue*: {reason}")
                lines.append(f"  - *Recommended Action*: {action}")

                # Add specific tuning suggestions
                details = rec.get("details", {})
                if details:
                    if "suggested_debounce_seconds" in details:
                        lines.append(f"  - *Suggested Debounce*: {details['suggested_debounce_seconds']:.0f} seconds")
                    if details.get("suggested_hysteresis"):
                        lines.append(f"  - *Consider Hysteresis*: Use separate up/down thresholds")

            lines.append("")

        # Benign transient policies
        transient_recs = recommendations.get("benign_transient_policies", [])
        if transient_recs:
            lines.append("##### ⚡ Benign Transient Policy Changes")
            lines.append("")
            lines.append("**Monitors with high rates of self-resolving transients:**")
            lines.append("")
            for rec in transient_recs[:5]:  # Top 5
                monitor_name = rec.get("monitor_name", "Unknown")
                monitor_id = rec.get("monitor_id")
                action = rec.get("action", "")
                reason = rec.get("reason", "")

                monitor_link = self._create_monitor_link(monitor_name, monitor_id)

                lines.append(f"- **{monitor_link}**")
                lines.append(f"  - *Pattern*: {reason}")
                lines.append(f"  - *Recommended Action*: {action}")

                # Add specific policy suggestions
                details = rec.get("details", {})
                if details.get("consider_dashboard_only"):
                    lines.append(f"  - *Notification Change*: Route to dashboard instead of alerting")
                if "suggested_notification_level" in details:
                    lines.append(f"  - *Severity Adjustment*: Change to '{details['suggested_notification_level']}' level")

            lines.append("")

        # General improvement suggestions
        lines.append("##### 📋 Implementation Guidelines")
        lines.append("")
        lines.append("**For Flapping Monitors:**")
        lines.append("1. **Increase Debounce Window**: Add `evaluation_delay` to prevent rapid state changes")
        lines.append("2. **Implement Hysteresis**: Use different thresholds for alert/recovery (e.g., alert at 90%, recover at 85%)")
        lines.append("3. **Consider Composite Monitors**: Combine multiple conditions to reduce noise")
        lines.append("4. **Window-Based Evaluation**: Use percentage of time over threshold rather than instant values")
        lines.append("")
        lines.append("**For Benign Transients:**")
        lines.append("1. **Adjust Notification Routing**: Send to dashboards instead of paging teams")
        lines.append("2. **Increase Alert Duration Threshold**: Require sustained conditions before alerting")
        lines.append("3. **Add Context**: Include automatic remediation hints in alert descriptions")
        lines.append("4. **Consider SLO-Based Alerts**: Focus on user-impacting issues rather than transient blips")
        lines.append("")

    def render_trend_analysis_section(self, trends: Dict[str, Any]) -> List[str]:
        """Render temporal trend analysis section."""
        lines = []

        if not trends:
            return lines

        lines.append("### 📈 Temporal Trend Analysis")
        lines.append("")
        lines.append("**Week-over-week analysis reveals monitoring system health trends:**")
        lines.append("")

        # Overall trend summary
        summary = trends.get("summary", {})
        if summary:
            analysis_week = summary.get("analysis_week", "Current")
            total_monitors = summary.get("total_monitors", 0)
            weeks_available = summary.get("weeks_available", 0)

            lines.append("#### Trend Summary")
            lines.append("")
            lines.append(f"- **Analysis Period**: Week {analysis_week}")
            lines.append(f"- **Monitors Analyzed**: {total_monitors}")
            lines.append(f"- **Historical Data**: {weeks_available} weeks available")
            lines.append("")

            # Trend distribution
            improving = summary.get("monitors_improving", 0)
            degrading = summary.get("monitors_degrading", 0)
            stable = summary.get("monitors_stable", 0)
            insufficient = summary.get("monitors_insufficient_data", 0)

            if total_monitors > 0:
                lines.append("**Monitor Trend Distribution:**")
                lines.append("")
                lines.append(f"- 🟢 **Improving**: {improving} monitors ({improving/total_monitors*100:.1f}%)")
                lines.append(f"- 🔴 **Degrading**: {degrading} monitors ({degrading/total_monitors*100:.1f}%)")
                lines.append(f"- ⚪ **Stable**: {stable} monitors ({stable/total_monitors*100:.1f}%)")
                if insufficient > 0:
                    lines.append(f"- ⚫ **Insufficient Data**: {insufficient} monitors ({insufficient/total_monitors*100:.1f}%)")
                lines.append("")

                # Highlight significant changes
                significant_changes = summary.get("significant_changes", [])
                if significant_changes:
                    lines.append("#### 📢 Significant Changes This Week")
                    lines.append("")
                    for change in significant_changes[:5]:  # Top 5
                        lines.append(f"- {change}")
                    lines.append("")

                # Top improvements and degradations
                top_improvements = summary.get("top_improvements", [])
                top_degradations = summary.get("top_degradations", [])

                if top_improvements:
                    lines.append("##### 🟢 Top Improvements")
                    lines.append("")
                    for improvement in top_improvements[:3]:
                        lines.append(f"- {improvement}")
                    lines.append("")

                if top_degradations:
                    lines.append("##### 🔴 Monitors Needing Attention")
                    lines.append("")
                    for degradation in top_degradations[:3]:
                        lines.append(f"- {degradation}")
                    lines.append("")

        # Individual monitor trends (sample)
        per_monitor = trends.get("per_monitor", {})
        if per_monitor:
            lines.append("#### Sample Monitor Trends")
            lines.append("")
            lines.append("| Monitor | Direction | Confidence | Key Changes |")
            lines.append("|---------|-----------|------------|-------------|")

            # Sort by trend confidence and show top examples
            sorted_monitors = sorted(
                per_monitor.items(),
                key=lambda x: x[1].trend_confidence,
                reverse=True
            )

            for monitor_id, trend_data in sorted_monitors[:5]:  # Top 5
                monitor_name = trend_data.monitor_name or monitor_id
                direction = trend_data.overall_direction.value
                confidence = trend_data.trend_confidence

                # Create trend emoji
                direction_emoji = {
                    TrendDirection.IMPROVING.value: "🟢",
                    TrendDirection.DEGRADING.value: "🔴",
                    TrendDirection.STABLE.value: "⚪",
                    TrendDirection.INSUFFICIENT_DATA.value: "⚫"
                }.get(direction, "❓")

                # Get key notable changes
                notable_changes = trend_data.notable_changes
                changes_text = "; ".join(notable_changes[:2]) if notable_changes else "No significant changes"
                if len(changes_text) > 50:
                    changes_text = changes_text[:47] + "..."

                monitor_link = self._create_monitor_link(monitor_name, monitor_id, max_length=25)

                lines.append(
                    f"| {monitor_link} | {direction_emoji} {direction} | {confidence:.2f} | {changes_text} |"
                )

            lines.append("")
            lines.append("💡 **Tip**: Focus on monitors with high confidence trends (>0.7) for actionable insights.")
            lines.append("")

        return lines

    def render_informational_policy_section(
        self,
        enhanced_quality: Dict[str, Any]
    ) -> List[str]:
        """Render informational-only policy recommendations section."""
        lines = []

        lines.append("### 📋 Informational-Only Policy Recommendations")
        lines.append("")
        lines.append("**Alerts that consistently exhibit benign behavior patterns should be considered for policy changes:**")
        lines.append("")

        per_monitor = enhanced_quality.get("per_monitor", {})
        if not per_monitor:
            return lines

        # Find monitors with high benign transient rates
        informational_candidates = []

        for monitor_id, metrics in per_monitor.items():
            benign_rate = metrics.get("benign_transient_rate", 0.0)
            confidence = metrics.get("classification_confidence", 0.0)
            total_cycles = metrics.get("cycle_count", 0)

            if benign_rate >= 0.7 and confidence >= 0.6 and total_cycles >= 5:
                informational_candidates.append({
                    "monitor_id": monitor_id,
                    "monitor_name": metrics.get("monitor_name"),
                    "benign_rate": benign_rate,
                    "confidence": confidence,
                    "total_cycles": total_cycles,
                    "business_hours_pct": metrics.get("business_hours_percentage", 0)
                })

        # Sort by benign rate
        informational_candidates.sort(key=lambda x: x["benign_rate"], reverse=True)

        if informational_candidates:
            lines.append("#### Candidates for Informational-Only Classification")
            lines.append("")
            lines.append("| Monitor | Benign Rate | Confidence | Business Hours Impact | Recommendation |")
            lines.append("|---------|-------------|------------|----------------------|----------------|")

            for candidate in informational_candidates[:10]:  # Top 10
                monitor_name = candidate.get("monitor_name", "Unknown")
                monitor_id = candidate["monitor_id"]
                benign_rate = candidate["benign_rate"]
                confidence = candidate["confidence"]
                bh_pct = candidate["business_hours_pct"]

                monitor_link = self._create_monitor_link(monitor_name, monitor_id, max_length=30)

                # Determine recommendation based on business hours impact
                if bh_pct > 50:
                    recommendation = "Dashboard + Low Priority Alert"
                else:
                    recommendation = "Dashboard Only"

                lines.append(
                    f"| {monitor_link} | {benign_rate:.1%} | {confidence:.2f} | {bh_pct:.1f}% | {recommendation} |"
                )

            lines.append("")
            lines.append("**Policy Change Guidelines:**")
            lines.append("")
            lines.append("- **Dashboard Only**: For monitors with <50% business hours impact")
            lines.append("- **Dashboard + Low Priority**: For business-critical services")
            lines.append("- **Increased Duration Threshold**: Require sustained conditions (5-10 minutes)")
            lines.append("- **Add Auto-Recovery Context**: Include hints about expected self-resolution")
            lines.append("")

        else:
            lines.append("No monitors currently qualify for informational-only classification.")
            lines.append("")
            lines.append("*Criteria: ≥70% benign transient rate, ≥60% classification confidence, ≥5 cycles*")
            lines.append("")

        return lines

    def render_configuration_impact_section(self, config_summary: Dict[str, Any]) -> List[str]:
        """Render section showing impact of current configuration."""
        lines = []

        lines.append("### ⚙️ Configuration Impact Analysis")
        lines.append("")
        lines.append("**Current analysis configuration and its impact on results:**")
        lines.append("")

        analysis_period = config_summary.get("analysis_period_days", 30)
        classification_enabled = config_summary.get("classification_enabled", False)
        trend_enabled = config_summary.get("trend_analysis_enabled", False)
        min_weeks = config_summary.get("min_weeks_for_trends", 3)

        lines.append(f"- **Analysis Period**: {analysis_period} days")
        lines.append(f"- **Enhanced Classification**: {'✅ Enabled' if classification_enabled else '❌ Disabled'}")
        lines.append(f"- **Trend Analysis**: {'✅ Enabled' if trend_enabled else '❌ Disabled'}")
        if trend_enabled:
            lines.append(f"- **Minimum Weeks for Trends**: {min_weeks} weeks")
        lines.append("")

        lines.append("**Configuration Recommendations:**")
        lines.append("")

        if analysis_period < 14:
            lines.append("🟡 **Consider Longer Analysis Period**: Current period may miss periodic patterns")
        elif analysis_period > 90:
            lines.append("🟡 **Consider Shorter Analysis Period**: Very long periods may dilute recent trends")
        else:
            lines.append("🟢 **Analysis Period**: Appropriate for reliable pattern detection")

        if not classification_enabled:
            lines.append("🔴 **Enable Enhanced Classification**: Get insights into flapping vs. benign vs. actionable alerts")

        if not trend_enabled:
            lines.append("🟡 **Enable Trend Analysis**: Track week-over-week improvements and degradations")

        lines.append("")

        return lines

    def _create_monitor_link(
        self,
        monitor_name: str,
        monitor_id: Optional[str],
        max_length: int = 40
    ) -> str:
        """Create a markdown link for a monitor."""
        clean_name = self._clean_monitor_name_for_markdown(monitor_name, max_length)

        if monitor_id and str(monitor_id).strip():
            link = f"[{clean_name}]({self.datadog_base_url}/monitors/{monitor_id})"
            return link

        return clean_name

    def _clean_monitor_name_for_markdown(self, monitor_name: str, max_length: int = 30) -> str:
        """Clean monitor name for safe use in Markdown tables."""
        if not monitor_name:
            return "Unknown"

        # Replace problematic Markdown characters
        clean_name = (monitor_name
                     .replace("|", "│")
                     .replace("[", "⟨")
                     .replace("]", "⟩")
                     .replace("(", "❨")
                     .replace(")", "❩")
                     .replace("#", "＃")
                     .replace("*", "✱")
                     .replace("_", "‿")
                     .replace("`", "ˋ")
                     .replace("~", "∼")
                     .replace("\\", "⧵")
                     .replace("\n", " ")
                     .replace("\r", " ")
                     .replace("\t", " ")
                     .strip())

        # Collapse multiple spaces
        clean_name = " ".join(clean_name.split())

        # Truncate if too long
        if len(clean_name) > max_length:
            return clean_name[:max_length] + "..."

        return clean_name