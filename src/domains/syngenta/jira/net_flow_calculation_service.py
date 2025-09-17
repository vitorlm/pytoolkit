"""
JIRA Net Flow Calculation Service

This service calculates net flow metrics by analyzing arrival rate (issues created)
versus throughput (issues completed) for specified time periods. It supports
generating a scorecard with a rolling 4-week trend analysis.

Net Flow = Arrival Rate - Throughput

- Positive Net Flow: More work is arriving than being completed (backlog may be growing).
- Negative Net Flow: More work is being completed than arriving (backlog may be shrinking).
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from collections import defaultdict
import math

from domains.syngenta.jira.issue_adherence_service import TimePeriodParser
from domains.syngenta.jira.workflow_config_service import WorkflowConfigService
from utils.data.json_manager import JSONManager
from utils.jira.jira_assistant import JiraAssistant
from utils.logging.logging_manager import LogManager
from utils.output_manager import OutputManager


@dataclass
class WeeklyFlowMetrics:
    """Data class to hold flow metrics for a single week."""

    week_number: int
    start_date: str
    end_date: str
    arrival_rate: int
    throughput: int
    net_flow: int


@dataclass
class NetFlowScorecard:
    """Data class for the complete net flow scorecard."""

    metadata: Dict
    current_week: WeeklyFlowMetrics
    rolling_trend: List[WeeklyFlowMetrics]
    insights: List[str]
    details: Dict = field(default_factory=dict)


class NetFlowCalculationService:
    """Service class for net flow calculations and scorecard generation."""

    def __init__(self):
        self.jira_assistant = JiraAssistant()
        self.workflow_service = WorkflowConfigService()
        self.time_parser = TimePeriodParser()
        self.logger = LogManager.get_instance().get_logger("NetFlowCalculationService")

    def generate_net_flow_scorecard(
        self,
        project_key: str,
        end_date: str,
        issue_types: Optional[List[str]] = None,
        team: Optional[str] = None,
        include_subtasks: bool = False,
        output_format: str = "console",
        verbose: bool = False,
    ) -> Dict:
        """
        Generates a net flow scorecard with a rolling 4-week trend.
        """
        try:
            self.logger.info(f"Generating Net Flow Scorecard for project {project_key} with anchor date {end_date}")

            done_statuses = self.workflow_service.get_done_statuses(project_key)
            issue_types = issue_types or ["Story", "Task", "Bug", "Epic"]

            anchor_dt = datetime.fromisoformat(end_date)
            days_until_sunday = (6 - anchor_dt.weekday()) % 7
            primary_end = anchor_dt + timedelta(days=days_until_sunday)
            primary_start = primary_end - timedelta(days=6)

            rolling_trend_metrics = []
            all_completed_issues = []
            for i in range(4):
                start_dt = primary_start - timedelta(weeks=i)
                end_dt = primary_end - timedelta(weeks=i)

                metrics, completed_issues = self._calculate_metrics_for_period(
                    project_key,
                    start_dt,
                    end_dt,
                    issue_types,
                    team,
                    include_subtasks,
                    done_statuses,
                )
                rolling_trend_metrics.append(metrics)
                if i == 0:
                    all_completed_issues = completed_issues

            rolling_trend_metrics.reverse()

            current_week_metrics = rolling_trend_metrics[-1]

            # Advanced Metrics Calculation
            flow_efficiency, bottleneck = self._analyze_flow_efficiency_and_bottlenecks(
                all_completed_issues, project_key
            )
            arrival_rates = [m["arrival_rate"] for m in rolling_trend_metrics]
            arrival_volatility = self._calculate_volatility(arrival_rates)

            flow_status = self._determine_flow_status(
                current_week_metrics["net_flow"],
                current_week_metrics["arrival_rate"],
                current_week_metrics["throughput"],
            )
            flow_ratio = (
                (current_week_metrics["throughput"] / current_week_metrics["arrival_rate"] * 100)
                if current_week_metrics["arrival_rate"] > 0
                else 0
            )

            response = {
                "metadata": {
                    "project_key": project_key,
                    "anchor_date": end_date,
                    "team": team,
                    "issue_types": issue_types,
                    "include_subtasks": include_subtasks,
                    "analysis_date": datetime.now().isoformat(),
                    "week_number": datetime.fromisoformat(current_week_metrics["start_date"]).isocalendar()[1],
                    "start_date": current_week_metrics["start_date"],
                    "end_date": current_week_metrics["end_date"],
                },
                "current_week": {
                    "arrival_rate": current_week_metrics["arrival_rate"],
                    "throughput": current_week_metrics["throughput"],
                    "net_flow": current_week_metrics["net_flow"],
                    "flow_ratio": flow_ratio,
                    "flow_status": flow_status,
                    "flow_efficiency": flow_efficiency,
                    "primary_bottleneck": bottleneck,
                    "arrival_volatility": arrival_volatility,
                },
                "rolling_4_weeks_trend": [
                    {
                        "week_number": datetime.fromisoformat(m["start_date"]).isocalendar()[1],
                        "net_flow": m["net_flow"],
                        "start_date": m["start_date"],
                        "end_date": m["end_date"],
                        "arrival_rate": m["arrival_rate"],
                        "throughput": m["throughput"],
                    }
                    for m in rolling_trend_metrics
                ],
                "insights": self._generate_insights(
                    current_week_metrics["net_flow"],
                    current_week_metrics["arrival_rate"],
                    current_week_metrics["throughput"],
                ),
                "details": {
                    "arrival_issues": current_week_metrics.get("arrival_issues", []),
                    "completed_issues": current_week_metrics.get("completed_issues", []),
                }
                if verbose
                else {},
            }

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            sub_dir = f"net-flow_{timestamp}"

            if output_format == "json":
                OutputManager.save_json_report(
                    response,
                    sub_dir,
                    f"net_flow_scorecard_{project_key}",
                )
            elif output_format == "md":
                markdown_content = self._format_as_markdown(response)
                OutputManager.save_markdown_report(
                    markdown_content,
                    sub_dir,
                    f"net_flow_scorecard_{project_key}",
                )

            self.logger.info("Net Flow Scorecard generation completed successfully.")
            return response

        except Exception as e:
            self.logger.error(f"Error in scorecard generation: {e}", exc_info=True)
            raise

    def _calculate_metrics_for_period(
        self,
        project_key: str,
        start_date: datetime,
        end_date: datetime,
        issue_types: List[str],
        team: Optional[str],
        include_subtasks: bool,
        done_statuses: List[str],
    ) -> tuple[Dict, List]:
        """Calculates arrival, throughput, and net flow for a single time period."""
        arrival_issues = self._fetch_created_issues(
            project_key, start_date, end_date, issue_types, team, include_subtasks
        )
        completed_issues = self._fetch_completed_issues(
            project_key, start_date, end_date, issue_types, team, include_subtasks, done_statuses
        )

        metrics = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "arrival_rate": len(arrival_issues),
            "throughput": len(completed_issues),
            "net_flow": len(arrival_issues) - len(completed_issues),
        }

        metrics["arrival_issues"] = [self._extract_issue_summary(i) for i in arrival_issues]
        metrics["completed_issues"] = [self._extract_issue_summary(i) for i in completed_issues]

        return metrics, completed_issues

    def _analyze_flow_efficiency_and_bottlenecks(
        self, completed_issues: List[Dict], project_key: str
    ) -> tuple[float, str]:
        # TODO: Move status configuration to WorkflowConfigService
        active_statuses = self.workflow_service.get_active_statuses(project_key)
        waiting_statuses = self.workflow_service.get_waiting_statuses(project_key)

        total_work_time_seconds = 0
        total_cycle_time_seconds = 0
        status_time_seconds = defaultdict(float)

        for issue in completed_issues:
            changelog = issue.get("changelog", {}).get("histories", [])
            if not changelog:
                continue

            issue_work_time = 0
            issue_cycle_time = 0
            first_active_time = None
            last_status_change_time = datetime.fromisoformat(issue["fields"]["created"])

            for history in changelog:
                history_time = datetime.fromisoformat(history["created"])
                for item in history["items"]:
                    if item["field"] == "status":
                        from_status = item["fromString"]

                        time_in_from_status = (history_time - last_status_change_time).total_seconds()

                        if from_status in active_statuses or from_status in waiting_statuses:
                            if first_active_time is None and from_status in active_statuses:
                                first_active_time = last_status_change_time

                            if first_active_time:
                                issue_cycle_time += time_in_from_status
                                status_time_seconds[from_status] += time_in_from_status
                                if from_status in active_statuses:
                                    issue_work_time += time_in_from_status

                        last_status_change_time = history_time

            total_work_time_seconds += issue_work_time
            total_cycle_time_seconds += issue_cycle_time

        flow_efficiency = (
            (total_work_time_seconds / total_cycle_time_seconds * 100) if total_cycle_time_seconds > 0 else 0
        )

        bottleneck = (
            max(status_time_seconds.keys(), key=lambda k: status_time_seconds[k]) if status_time_seconds else "N/A"
        )

        return flow_efficiency, bottleneck

    def _calculate_volatility(self, data: List[float]) -> float:
        if not data or len(data) < 2:
            return 0.0

        mean = sum(data) / len(data)
        if mean == 0:
            return 0.0

        variance = sum([(x - mean) ** 2 for x in data]) / len(data)
        std_dev = math.sqrt(variance)

        return (std_dev / mean) * 100

    def _fetch_created_issues(
        self,
        project_key: str,
        start_date: datetime,
        end_date: datetime,
        issue_types: list,
        team: Optional[str],
        include_subtasks: bool,
    ) -> list:
        """Fetch issues created in the specified time period."""
        jql_parts = [f"project = '{project_key}'"]
        if issue_types:
            types_str = "', '".join(issue_types)
            jql_parts.append(f"type in ('{types_str}')")
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        jql_parts.append(f"created >= '{start_date_str}' AND created <= '{end_date_str}'")
        if team:
            squad_field = self.workflow_service.get_custom_field(project_key, "squad_field")
            if squad_field:
                jql_parts.append(f"'{squad_field}' = '{team}'")
        if not include_subtasks:
            jql_parts.append("type != Sub-task")
        jql_query = " AND ".join(jql_parts)
        self.logger.info(f"Fetching created issues with JQL: {jql_query}")
        issues = self.jira_assistant.fetch_issues(
            jql_query=jql_query,
            fields="key,summary,issuetype,status,created,assignee,customfield_10265",
            max_results=1000,
            expand_changelog=False,
        )
        self.logger.info(f"Found {len(issues)} issues created in period")
        return issues

    def _fetch_completed_issues(
        self,
        project_key: str,
        start_date: datetime,
        end_date: datetime,
        issue_types: list,
        team: Optional[str],
        include_subtasks: bool,
        done_statuses: list,
    ) -> list:
        """Fetch issues completed in the specified time period."""
        jql_parts = [f"project = '{project_key}'"]
        if issue_types:
            types_str = "', '".join(issue_types)
            jql_parts.append(f"type in ('{types_str}')")
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        jql_parts.append(f"resolved >= '{start_date_str}' AND resolved <= '{end_date_str}'")
        if done_statuses:
            status_str = "', '".join(done_statuses)
            jql_parts.append(f"status in ('{status_str}')")
        if team:
            squad_field = self.workflow_service.get_custom_field(project_key, "squad_field")
            if squad_field:
                jql_parts.append(f"'{squad_field}' = '{team}'")
        if not include_subtasks:
            jql_parts.append("type != Sub-task")
        jql_query = " AND ".join(jql_parts)
        self.logger.info(f"Fetching completed issues with JQL: {jql_query}")
        issues = self.jira_assistant.fetch_issues(
            jql_query=jql_query,
            fields="key,summary,issuetype,status,created,resolved,assignee,customfield_10265",
            max_results=1000,
            expand_changelog=True,
        )
        self.logger.info(f"Found {len(issues)} issues completed in period")
        return issues

    def _extract_issue_summary(self, issue: dict) -> dict:
        """Extract summary information from an issue."""
        fields = issue.get("fields", {})
        return {
            "key": issue.get("key"),
            "summary": fields.get("summary"),
            "type": fields.get("issuetype", {}).get("name"),
            "status": fields.get("status", {}).get("name"),
            "assignee": fields.get("assignee", {}).get("displayName") if fields.get("assignee") else None,
        }

    def _determine_flow_status(self, net_flow: int, arrival_rate: int, throughput: int) -> str:
        """Determine flow status based on net flow value."""
        if net_flow > 0:
            if net_flow > (arrival_rate * 0.2):  # More than 20% imbalance
                return "CRITICAL_BOTTLENECK"
            else:
                return "MINOR_BOTTLENECK"
        elif net_flow < 0:
            return "HEALTHY_FLOW"
        else:
            return "BALANCED"

    def _generate_insights(self, net_flow: int, arrival_rate: int, throughput: int) -> list:
        """Generate insights based on flow metrics."""
        insights = []
        if net_flow > 0:
            insights.append(
                f"⚠️  Work is arriving faster than being completed ({net_flow} more arrivals than completions)"
            )
            insights.append("💡 Consider: increasing team capacity, reducing scope, or improving process efficiency")
            if arrival_rate > 0:
                backlog_growth_rate = (net_flow / arrival_rate) * 100
                insights.append(f"📈 Backlog growing at {backlog_growth_rate:.1f}% rate relative to arrival rate")
        elif net_flow < 0:
            insights.append(f"✅ Healthy flow: completing more work than arriving ({abs(net_flow)} more completions)")
            insights.append("💡 Consider: taking on additional work or focusing on higher-value items")
        else:
            insights.append("⚖️  Perfectly balanced: arrival rate equals throughput")
        if arrival_rate > 0:
            efficiency = (throughput / arrival_rate) * 100
            if efficiency < 50:
                insights.append(f"🐌 Low throughput efficiency: {efficiency:.1f}% - investigate bottlenecks")
            elif efficiency > 120:
                insights.append(f"🚀 High throughput efficiency: {efficiency:.1f}% - sustainable pace?")
        if arrival_rate == 0 and throughput == 0:
            insights.append("🔍 No activity detected in this period - check filters or time range")
        elif arrival_rate == 0:
            insights.append("📉 No new work arriving - focus on completing existing backlog")
        elif throughput == 0:
            insights.append("🚫 No work being completed - investigate delivery blockers")
        return insights

    def _save_results(self, results: dict, output_file: str):
        """Save results to JSON file."""
        try:
            JSONManager.write_json(results, output_file)
            self.logger.info(f"Net flow results saved to {output_file}")
        except Exception as e:
            self.logger.error(f"Failed to save results to {output_file}: {e}")
            raise

    def _format_as_markdown(self, scorecard: Dict) -> str:
        """
        Formats the net flow scorecard as structured markdown optimized for AI agent consumption.

        Args:
            scorecard (Dict): The complete scorecard data structure

        Returns:
            str: Markdown formatted content optimized for AI parsing
        """
        metadata = scorecard.get("metadata", {})
        current_week = scorecard.get("current_week", {})
        trend = scorecard.get("rolling_4_weeks_trend", [])
        insights = scorecard.get("insights", [])
        details = scorecard.get("details", {})

        # Header section
        project_key = metadata.get("project_key", "N/A")
        week_number = metadata.get("week_number", "N/A")
        start_date = metadata.get("start_date", "")
        end_date = metadata.get("end_date", "")
        anchor_date = metadata.get("anchor_date", "")
        team = metadata.get("team", "All Teams")

        markdown_content = f"""# JIRA Net Flow Analysis Report

## Executive Summary
- **Project**: {project_key}
- **Analysis Period**: Week {week_number} ({start_date} to {end_date})
- **Anchor Date**: {anchor_date}
- **Team Scope**: {team}
- **Analysis Date**: {metadata.get("analysis_date", "")}

## Current Week Metrics

### Flow Overview
| Metric | Value | Status |
|--------|-------|--------|
| **Net Flow** | {current_week.get("net_flow", 0):+d} | {self._get_flow_status_emoji(current_week.get("net_flow", 0))} |
| **Arrival Rate** | {current_week.get("arrival_rate", 0)} issues | 📥 |
| **Throughput** | {current_week.get("throughput", 0)} issues | ✅ |
| **Flow Ratio** | {current_week.get("flow_ratio", 0):.1f}% | {self._get_ratio_status_emoji(current_week.get("flow_ratio", 0))} |

### Advanced Metrics
| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| **Flow Efficiency** | {current_week.get("flow_efficiency", 0):.1f}% | >40% | {self._get_efficiency_status_emoji(current_week.get("flow_efficiency", 0))} |
| **Arrival Volatility** | {current_week.get("arrival_volatility", 0):.1f}% | <30% | {self._get_volatility_status_emoji(current_week.get("arrival_volatility", 0))} |
| **Primary Bottleneck** | {current_week.get("primary_bottleneck", "N/A")} | - | ⚠️ |
| **Overall Flow Status** | {current_week.get("flow_status", "UNKNOWN")} | HEALTHY_FLOW | {self._get_overall_status_emoji(current_week.get("flow_status", ""))} |

## Rolling 4-Week Trend Analysis

### Trend Data
"""

        # Add trend table
        if trend:
            markdown_content += "| Week | Net Flow | Arrival | Throughput | Trend |\n"
            markdown_content += "|------|----------|---------|------------|-------|\n"

            for i, week_data in enumerate(trend):
                week_num = week_data["week_number"]
                net_flow = week_data["net_flow"]
                arrival = week_data["arrival_rate"]
                throughput = week_data["throughput"]

                # Calculate trend indicator
                trend_indicator = ""
                if i > 0:
                    prev_net_flow = trend[i - 1]["net_flow"]
                    if net_flow < prev_net_flow:
                        trend_indicator = "📈 Improving"
                    elif net_flow > prev_net_flow:
                        trend_indicator = "📉 Worsening"
                    else:
                        trend_indicator = "➡️ Stable"

                is_current = i == len(trend) - 1
                week_indicator = "**" if is_current else ""

                markdown_content += f"| {week_indicator}Week {week_num}{week_indicator} | {net_flow:+d} | {arrival} | {throughput} | {trend_indicator} |\n"

        # Insights section
        markdown_content += """

## AI-Actionable Insights

### Key Findings
"""

        for i, insight in enumerate(insights, 1):
            markdown_content += f"{i}. {insight}\n"

        # Detailed analysis for verbose mode
        if details:
            arrival_issues = details.get("arrival_issues", [])
            completed_issues = details.get("completed_issues", [])

            markdown_content += f"""

## Detailed Issue Analysis

### Arrival Issues ({len(arrival_issues)} total)
"""
            if arrival_issues:
                markdown_content += "| Key | Type | Summary | Assignee |\n"
                markdown_content += "|-----|------|---------|----------|\n"

                for issue in arrival_issues[:10]:  # Limit to first 10 for readability
                    key = issue.get("key", "N/A")
                    issue_type = issue.get("type", "N/A")
                    summary = issue.get("summary", "")[:60] + ("..." if len(issue.get("summary", "")) > 60 else "")
                    assignee = issue.get("assignee") or "Unassigned"
                    markdown_content += f"| {key} | {issue_type} | {summary} | {assignee} |\n"

                if len(arrival_issues) > 10:
                    markdown_content += f"\n*... and {len(arrival_issues) - 10} more issues*\n"

            markdown_content += f"""

### Completed Issues ({len(completed_issues)} total)
"""
            if completed_issues:
                markdown_content += "| Key | Type | Summary | Assignee |\n"
                markdown_content += "|-----|------|---------|----------|\n"

                for issue in completed_issues[:10]:  # Limit to first 10 for readability
                    key = issue.get("key", "N/A")
                    issue_type = issue.get("type", "N/A")
                    summary = issue.get("summary", "")[:60] + ("..." if len(issue.get("summary", "")) > 60 else "")
                    assignee = issue.get("assignee") or "Unassigned"
                    markdown_content += f"| {key} | {issue_type} | {summary} | {assignee} |\n"

                if len(completed_issues) > 10:
                    markdown_content += f"\n*... and {len(completed_issues) - 10} more issues*\n"

        # Recommendations section
        markdown_content += f"""

## AI Agent Recommendations

### Flow Health Assessment
- **Current Status**: {current_week.get("flow_status", "UNKNOWN")}
- **Net Flow**: {current_week.get("net_flow", 0):+d} ({"Backlog Growing" if current_week.get("net_flow", 0) > 0 else "Backlog Shrinking" if current_week.get("net_flow", 0) < 0 else "Balanced"})
- **Flow Efficiency**: {current_week.get("flow_efficiency", 0):.1f}% ({"Above Target" if current_week.get("flow_efficiency", 0) > 40 else "Below Target"})

### Next Actions
"""

        # Generate contextual recommendations
        net_flow = current_week.get("net_flow", 0)
        flow_ratio = current_week.get("flow_ratio", 0)
        flow_efficiency = current_week.get("flow_efficiency", 0)

        if net_flow > 5:
            markdown_content += "1. **CRITICAL**: Implement flow throttling - backlog growing critically\n"
            markdown_content += "2. **URGENT**: Investigate delivery bottlenecks\n"
            markdown_content += "3. **ACTION**: Consider increasing team capacity or reducing incoming work\n"
        elif net_flow > 0:
            markdown_content += "1. **MONITOR**: Watch backlog growth trend closely\n"
            markdown_content += "2. **OPTIMIZE**: Focus on completing existing work before taking new items\n"
        elif net_flow < 0:
            markdown_content += "1. **POSITIVE**: Backlog is shrinking - sustainable pace\n"
            markdown_content += "2. **MONITOR**: Ensure adequate work pipeline for next period\n"
        else:
            markdown_content += "1. **BALANCED**: Maintain current flow patterns\n"

        if flow_ratio < 85:
            markdown_content += f"4. **EFFICIENCY**: Flow ratio at {flow_ratio:.0f}% - target >85%\n"

        if flow_efficiency < 40:
            markdown_content += f"5. **PROCESS**: Flow efficiency at {flow_efficiency:.0f}% - investigate wait times\n"

        markdown_content += f"""

## Data Structure (JSON)

```json
{{
    "metadata": {{
        "project_key": "{project_key}",
        "week_number": {week_number},
        "anchor_date": "{anchor_date}",
        "team": "{team}"
    }},
    "current_week": {{
        "net_flow": {current_week.get("net_flow", 0)},
        "arrival_rate": {current_week.get("arrival_rate", 0)},
        "throughput": {current_week.get("throughput", 0)},
        "flow_ratio": {current_week.get("flow_ratio", 0):.1f},
        "flow_efficiency": {current_week.get("flow_efficiency", 0):.1f},
        "flow_status": "{current_week.get("flow_status", "UNKNOWN")}"
    }}
}}
```

---
*Generated by PyToolkit JIRA Net Flow Analysis Service*
"""

        return markdown_content

    def _get_flow_status_emoji(self, net_flow: int) -> str:
        """Get emoji for net flow status."""
        if net_flow > 5:
            return "🚨"
        elif net_flow > 0:
            return "⚠️"
        elif net_flow == 0:
            return "✅"
        else:
            return "✅"

    def _get_ratio_status_emoji(self, flow_ratio: float) -> str:
        """Get emoji for flow ratio status."""
        if flow_ratio >= 85:
            return "✅"
        elif flow_ratio >= 70:
            return "⚠️"
        else:
            return "🚨"

    def _get_efficiency_status_emoji(self, flow_efficiency: float) -> str:
        """Get emoji for flow efficiency status."""
        if flow_efficiency >= 40:
            return "✅"
        elif flow_efficiency >= 25:
            return "⚠️"
        else:
            return "🚨"

    def _get_volatility_status_emoji(self, volatility: float) -> str:
        """Get emoji for arrival volatility status."""
        if volatility <= 30:
            return "✅"
        elif volatility <= 50:
            return "⚠️"
        else:
            return "🚨"

    def _get_overall_status_emoji(self, flow_status: str) -> str:
        """Get emoji for overall flow status."""
        status_emojis = {
            "CRITICAL_BOTTLENECK": "🚨",
            "MINOR_BOTTLENECK": "⚠️",
            "HEALTHY_FLOW": "✅",
            "BALANCED": "⚖️",
        }
        return status_emojis.get(flow_status, "❓")
