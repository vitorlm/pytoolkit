from dataclasses import dataclass
from datetime import UTC, datetime

import numpy as np

from domains.syngenta.jira.workflow_config_service import WorkflowConfigService
from utils.cache_manager.cache_manager import CacheManager
from utils.data.json_manager import JSONManager
from utils.jira.jira_assistant import JiraAssistant
from utils.logging.logging_manager import LogManager
from utils.output_manager import OutputManager


@dataclass
class WipIssueAge:
    """Data class for WIP issue age information."""

    key: str
    summary: str
    status: str
    assignee: str
    age_days: int
    status_since: str
    team: str


class WipAgeTrackingService:
    """Service for tracking WIP issue aging."""

    def __init__(self):
        self.jira_assistant = JiraAssistant()
        self.workflow_config = WorkflowConfigService()
        self.cache = CacheManager.get_instance()
        self.logger = LogManager.get_instance().get_logger("WipAgeTrackingService")

    def calculate_wip_age(
        self,
        project_key: str,
        alert_threshold: int = 5,
        team: str | None = None,
        issue_types: list | None = None,
        include_subtasks: bool = False,
        output_format: str = "console",
        output_file: str | None = None,
        verbose: bool = False,
    ) -> dict:
        """Calculate WIP aging metrics and alerts.

        Args:
            project_key: JIRA project key
            alert_threshold: Days threshold for aging alerts
            team: Optional team filter
            issue_types: List of issue types to include
            include_subtasks: Whether to include subtasks
            output_file: Optional output file path
            verbose: Enable verbose output

        Returns:
            dict: WIP aging analysis results
        """
        self.logger.info(f"Starting WIP age analysis for project {project_key}")

        # Check cache first
        cache_key = f"wip_age_{project_key}_{team or 'all'}_{hash(str(issue_types))}"
        cached_data = self.cache.load(cache_key, expiration_minutes=30)

        if cached_data is None:
            # Fetch fresh data
            wip_issues = self._fetch_wip_issues(project_key, team, issue_types, include_subtasks)
            self.cache.save(cache_key, wip_issues)
        else:
            self.logger.info("Using cached WIP age data")
            wip_issues = cached_data

        # Calculate age metrics
        results = self._analyze_wip_aging(wip_issues, project_key, alert_threshold, team, verbose)

        # Save results based on output format
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        sub_dir = f"wip-age_{timestamp}"

        if output_format == "json":
            OutputManager.save_json_report(
                results,
                sub_dir,
                f"wip_age_{project_key}",
            )
            self.logger.info("WIP age analysis results saved as JSON")
        elif output_format == "md":
            markdown_content = self._format_as_markdown(results)
            OutputManager.save_markdown_report(
                markdown_content,
                sub_dir,
                f"wip_age_{project_key}",
            )
            self.logger.info("WIP age analysis results saved as Markdown")

        # Legacy output file support
        if output_file:
            JSONManager.write_json(results, output_file)
            self.logger.info(f"Results saved to {output_file}")

        self.logger.info("WIP Age Analysis generation completed successfully.")
        return results

    def _fetch_wip_issues(
        self,
        project_key: str,
        team: str | None = None,
        issue_types: list | None = None,
        include_subtasks: bool = False,
    ) -> list:
        """Fetch issues currently in WIP statuses."""
        # Get WIP statuses from workflow configuration
        wip_statuses = self.workflow_config.get_wip_statuses(project_key)
        if not wip_statuses:
            self.logger.warning(f"No WIP statuses configured for project {project_key}")
            return []

        # Build JQL query
        jql_parts = [f"project = '{project_key}'"]

        # WIP statuses filter
        status_str = "', '".join(wip_statuses)
        jql_parts.append(f"status in ('{status_str}')")

        # Team filter
        if team:
            jql_parts.append(f"'Squad[Dropdown]' = '{team}'")

        # Issue types filter
        if issue_types:
            types_str = "', '".join(issue_types)
            jql_parts.append(f"type in ('{types_str}')")

        # Subtasks filter
        if not include_subtasks:
            jql_parts.append("type != Sub-task")

        jql_query = " AND ".join(jql_parts)
        self.logger.info(f"Fetching WIP issues with JQL: {jql_query}")

        # Get squad field for data extraction
        squad_field = self.workflow_config.get_custom_field(project_key, "squad_field")

        # Fetch issues with changelog for age calculation
        fields_to_fetch = (
            f"key,summary,status,created,assignee,{squad_field}"
            if squad_field
            else "key,summary,status,created,assignee"
        )

        # Try to fetch with changelog first, but handle timeout gracefully
        try:
            self.logger.info("Attempting to fetch issues with changelog for accurate age calculation")
            issues = self.jira_assistant.fetch_issues(
                jql_query=jql_query,
                fields=fields_to_fetch,
                expand_changelog=True,
                max_results=100,  # Limit to avoid timeout
            )
            self.logger.info("Successfully fetched issues with changelog data")
        except Exception as e:
            self.logger.warning(f"Failed to fetch with changelog: {e!s}")
            self.logger.info(
                "Falling back to basic fetch without changelog (will use created date for age calculation)"
            )
            issues = self.jira_assistant.fetch_issues(
                jql_query=jql_query,
                fields=fields_to_fetch,
                expand_changelog=False,
                max_results=100,
            )

        self.logger.info(f"Found {len(issues)} WIP issues")
        return issues

    def _analyze_wip_aging(
        self,
        issues: list,
        project_key: str,
        alert_threshold: int,
        team: str | None = None,
        verbose: bool = False,
    ) -> dict:
        """Analyze aging patterns for WIP issues."""
        if not issues:
            return self._empty_results(project_key, alert_threshold, team)

        # Get WIP statuses and squad field for reference
        wip_statuses = self.workflow_config.get_wip_statuses(project_key)
        squad_field = self.workflow_config.get_custom_field(project_key, "squad_field")

        # Calculate age for each issue
        wip_issue_ages = []
        status_breakdown = {}
        alerts = []

        for issue in issues:
            age_info = self._calculate_issue_age(issue, wip_statuses)

            # Extract issue details
            fields = issue.get("fields", {})
            assignee = fields.get("assignee", {})
            assignee_name = assignee.get("displayName", "Unassigned") if assignee else "Unassigned"

            # Get team from Squad field using dynamic field from config
            team_value = fields.get(squad_field, {}) if squad_field else {}
            team_name = team_value.get("value", "No Team") if team_value else "No Team"

            wip_issue = WipIssueAge(
                key=issue["key"],
                summary=fields.get("summary", ""),
                status=fields.get("status", {}).get("name", ""),
                assignee=assignee_name,
                age_days=age_info["current_status_age_days"],
                status_since=age_info["current_status_start"] or "",
                team=team_name,
            )

            wip_issue_ages.append(wip_issue)

            # Check for alerts
            if wip_issue.age_days >= alert_threshold:
                alerts.append(
                    {
                        "key": wip_issue.key,
                        "summary": wip_issue.summary,
                        "status": wip_issue.status,
                        "age_days": wip_issue.age_days,
                        "assignee": wip_issue.assignee,
                        "status_since": wip_issue.status_since,
                    }
                )

            # Status breakdown
            status = wip_issue.status
            if status not in status_breakdown:
                status_breakdown[status] = {
                    "issues": [],
                    "total_age": 0,
                    "issues_over_threshold": 0,
                }

            status_breakdown[status]["issues"].append(wip_issue)
            status_breakdown[status]["total_age"] += wip_issue.age_days
            if wip_issue.age_days >= alert_threshold:
                status_breakdown[status]["issues_over_threshold"] += 1

        # Calculate statistics
        ages = [issue.age_days for issue in wip_issue_ages]
        statistics = self._calculate_aging_statistics(ages)

        # Generate status breakdown summary
        status_summary = {}
        for status, data in status_breakdown.items():
            issue_count = len(data["issues"])
            status_summary[status] = {
                "issue_count": issue_count,
                "avg_age_days": round(data["total_age"] / issue_count, 1) if issue_count > 0 else 0,
                "issues_over_threshold": data["issues_over_threshold"],
            }

        # Generate insights
        insights = self._generate_insights(wip_issue_ages, status_summary, alert_threshold, statistics)

        # Build results
        results = {
            "metadata": {
                "project_key": project_key,
                "alert_threshold_days": alert_threshold,
                "team": team,
                "analysis_date": datetime.now(UTC).isoformat(),
                "wip_statuses": wip_statuses,
            },
            "summary": {
                "total_wip_issues": len(wip_issue_ages),
                "issues_over_threshold": len(alerts),
                "average_age_days": round(statistics.get("mean", 0), 1),
                "median_age_days": round(statistics.get("median", 0), 1),
                "oldest_issue_age_days": statistics.get("max", 0),
            },
            "statistics": {"age_distribution": statistics},
            "status_breakdown": status_summary,
            "alerts": {"issues_over_threshold": alerts},
            "insights": insights,
        }

        # Add detailed issue list if verbose
        if verbose:
            results["detailed_issues"] = [
                {
                    "key": issue.key,
                    "summary": issue.summary,
                    "status": issue.status,
                    "age_days": issue.age_days,
                    "assignee": issue.assignee,
                    "team": issue.team,
                    "status_since": issue.status_since,
                }
                for issue in wip_issue_ages
            ]

        return results

    def _calculate_issue_age(self, issue: dict, wip_statuses: list) -> dict:
        """Calculate age metrics for an issue in WIP."""
        changelog = issue.get("changelog", {}).get("histories", [])
        current_status = issue["fields"]["status"]["name"]

        # Find last transition to current WIP status
        current_status_start = None
        fallback_reason = None

        # Only try changelog if it exists and has data
        if changelog:
            for history in reversed(changelog):
                for item in history.get("items", []):
                    if item.get("field") == "status":
                        if item.get("toString") == current_status:
                            try:
                                # Parse JIRA timestamp
                                created_str = history["created"]
                                if created_str.endswith("Z"):
                                    created_str = created_str[:-1] + "+00:00"
                                current_status_start = datetime.fromisoformat(created_str)
                                break
                            except (ValueError, KeyError) as e:
                                self.logger.warning(f"Error parsing timestamp for {issue['key']}: {e}")
                                continue

                if current_status_start:
                    break
        else:
            fallback_reason = "no_changelog"

        # If no transition found in changelog, use created date as fallback
        if not current_status_start:
            try:
                created_str = issue["fields"]["created"]
                if created_str.endswith("Z"):
                    created_str = created_str[:-1] + "+00:00"
                current_status_start = datetime.fromisoformat(created_str)
                if not fallback_reason:
                    fallback_reason = "no_status_transition_found"
            except (ValueError, KeyError):
                current_status_start = datetime.now(UTC)
                fallback_reason = "parse_error"

        # Log fallback usage for debugging
        if fallback_reason:
            self.logger.debug(f"Using fallback for {issue['key']}: {fallback_reason}")

        # Calculate age in current status
        now = datetime.now(UTC)
        current_age_days = (now - current_status_start).days

        return {
            "current_status_age_days": max(0, current_age_days),
            "current_status_start": current_status_start.isoformat() if current_status_start else None,
            "total_wip_time_days": 0,  # Could be extended for full WIP time calculation
        }

    def _calculate_aging_statistics(self, ages: list) -> dict:
        """Calculate statistical metrics for WIP ages."""
        if not ages:
            return {}

        ages_array = np.array(ages)

        return {
            "count": len(ages),
            "mean": float(np.mean(ages_array)),
            "median": float(np.median(ages_array)),
            "std_dev": float(np.std(ages_array)),
            "percentiles": {
                "p50": float(np.percentile(ages_array, 50)),
                "p75": float(np.percentile(ages_array, 75)),
                "p90": float(np.percentile(ages_array, 90)),
                "p95": float(np.percentile(ages_array, 95)),
            },
            "min": int(min(ages)),
            "max": int(max(ages)),
        }

    def _generate_insights(
        self,
        wip_issues: list,
        status_summary: dict,
        alert_threshold: int,
        statistics: dict,
    ) -> list:
        """Generate actionable insights about WIP aging."""
        insights = []

        if not wip_issues:
            return ["No WIP issues found for analysis"]

        total_issues = len(wip_issues)
        over_threshold = len([issue for issue in wip_issues if issue.age_days >= alert_threshold])

        # Threshold analysis
        if over_threshold > 0:
            percentage = (over_threshold / total_issues) * 100
            insights.append(
                f"{over_threshold} issues ({percentage:.1f}%) exceed the {alert_threshold}-day aging threshold"
            )

        # Status bottleneck analysis
        if status_summary:
            max_avg_age = max(status_summary.values(), key=lambda x: x["avg_age_days"])
            max_status = next(k for k, v in status_summary.items() if v == max_avg_age)
            if max_avg_age["avg_age_days"] > alert_threshold:
                insights.append(
                    f"{max_status} status shows highest average age ({max_avg_age['avg_age_days']:.1f} days) - potential bottleneck"
                )

        # Percentile analysis
        p90 = statistics.get("percentiles", {}).get("p90", 0)
        if p90 > alert_threshold * 1.5:
            insights.append(f"P90 age of {p90:.0f} days indicates some issues aging significantly")

        # Actionable recommendations
        if over_threshold > 0:
            insights.append("Consider reviewing issues over threshold for potential blockers")

        if not insights:
            insights.append("WIP flow appears healthy with no significant aging concerns")

        return insights

    def _empty_results(self, project_key: str, alert_threshold: int, team: str | None = None) -> dict:
        """Return empty results structure when no issues found."""
        return {
            "metadata": {
                "project_key": project_key,
                "alert_threshold_days": alert_threshold,
                "team": team,
                "analysis_date": datetime.now(UTC).isoformat(),
                "wip_statuses": [],
            },
            "summary": {
                "total_wip_issues": 0,
                "issues_over_threshold": 0,
                "average_age_days": 0,
                "median_age_days": 0,
                "oldest_issue_age_days": 0,
            },
            "statistics": {"age_distribution": {}},
            "status_breakdown": {},
            "alerts": {"issues_over_threshold": []},
            "insights": ["No WIP issues found for analysis"],
        }

    def _format_as_markdown(self, results: dict) -> str:
        """Formats the WIP age tracking results as structured markdown optimized for AI agent consumption.

        Args:
            results (dict): The complete WIP analysis data structure

        Returns:
            str: Markdown formatted content optimized for AI parsing
        """
        metadata = results.get("metadata", {})
        summary = results.get("summary", {})
        statistics = results.get("statistics", {}).get("age_distribution", {})
        status_breakdown = results.get("status_breakdown", {})
        alerts = results.get("alerts", {})
        insights = results.get("insights", [])
        detailed_issues = results.get("detailed_issues", [])

        # Header section
        project_key = metadata.get("project_key", "N/A")
        alert_threshold = metadata.get("alert_threshold_days", 5)
        team = metadata.get("team", "All Teams")
        analysis_date = metadata.get("analysis_date", "")
        wip_statuses = metadata.get("wip_statuses", [])

        markdown_content = f"""# JIRA WIP Age Tracking Report

## Executive Summary
- **Project**: {project_key}
- **Alert Threshold**: {alert_threshold} days
- **Team Scope**: {team}
- **Analysis Date**: {analysis_date}
- **WIP Statuses**: {", ".join(wip_statuses)}

## WIP Age Overview

### Key Metrics
| Metric | Value | Status |
|--------|-------|--------|
| **Total WIP Issues** | {summary.get("total_wip_issues", 0)} | 📊 |
| **Issues Over Threshold** | {summary.get("issues_over_threshold", 0)} | {self._get_alert_status_emoji(summary.get("issues_over_threshold", 0))} |
| **Average Age** | {summary.get("average_age_days", 0):.1f} days | {self._get_age_status_emoji(summary.get("average_age_days", 0), alert_threshold)} |
| **Median Age** | {summary.get("median_age_days", 0):.1f} days | {self._get_age_status_emoji(summary.get("median_age_days", 0), alert_threshold)} |
| **Oldest Issue Age** | {summary.get("oldest_issue_age_days", 0)} days | {self._get_age_status_emoji(summary.get("oldest_issue_age_days", 0), alert_threshold * 2)} |

## Age Distribution Statistics
"""

        # Add statistics if available
        if statistics:
            percentiles = statistics.get("percentiles", {})
            markdown_content += f"""
| Percentile | Age (days) | Assessment |
|------------|------------|------------|
| **P50 (Median)** | {percentiles.get("p50", 0):.1f} | {self._get_percentile_assessment(percentiles.get("p50", 0), alert_threshold)} |
| **P75** | {percentiles.get("p75", 0):.1f} | {self._get_percentile_assessment(percentiles.get("p75", 0), alert_threshold)} |
| **P90** | {percentiles.get("p90", 0):.1f} | {self._get_percentile_assessment(percentiles.get("p90", 0), alert_threshold)} |
| **P95** | {percentiles.get("p95", 0):.1f} | {self._get_percentile_assessment(percentiles.get("p95", 0), alert_threshold)} |

### Distribution Details
- **Count**: {statistics.get("count", 0)} issues
- **Standard Deviation**: {statistics.get("std_dev", 0):.1f} days
- **Min Age**: {statistics.get("min", 0)} days
- **Max Age**: {statistics.get("max", 0)} days
"""

        # Status breakdown
        if status_breakdown:
            markdown_content += """

## Status Breakdown Analysis

| Status | Issue Count | Avg Age (days) | Issues Over Threshold | Health |
|--------|-------------|----------------|----------------------|--------|
"""
            for status, data in status_breakdown.items():
                issue_count = data.get("issue_count", 0)
                avg_age = data.get("avg_age_days", 0)
                over_threshold = data.get("issues_over_threshold", 0)
                health_status = self._get_status_health_emoji(over_threshold, issue_count)

                markdown_content += (
                    f"| {status} | {issue_count} | {avg_age:.1f} | {over_threshold} | {health_status} |\n"
                )

        # Alerts section
        alert_issues = alerts.get("issues_over_threshold", [])
        if alert_issues:
            markdown_content += f"""

## 🚨 Issues Requiring Attention ({len(alert_issues)} issues)

| Key | Summary | Status | Age | Assignee |
|-----|---------|--------|-----|----------|
"""
            for issue in alert_issues[:15]:  # Limit to first 15 for readability
                key = issue.get("key", "N/A")
                summary = issue.get("summary", "")[:50] + ("..." if len(issue.get("summary", "")) > 50 else "")
                status = issue.get("status", "N/A")
                age = issue.get("age_days", 0)
                assignee = issue.get("assignee") or "Unassigned"
                markdown_content += f"| {key} | {summary} | {status} | {age}d | {assignee} |\n"

            if len(alert_issues) > 15:
                markdown_content += f"\n*... and {len(alert_issues) - 15} more issues over threshold*\n"

        # AI-Actionable Insights
        markdown_content += """

## AI-Actionable Insights

### Key Findings
"""

        for i, insight in enumerate(insights, 1):
            markdown_content += f"{i}. {insight}\n"

        # Detailed issue analysis for verbose mode
        if detailed_issues:
            markdown_content += f"""

## Detailed WIP Issues Analysis ({len(detailed_issues)} total)

### All WIP Issues
| Key | Summary | Status | Age | Assignee | Team |
|-----|---------|--------|-----|----------|------|
"""
            for issue in detailed_issues[:20]:  # Limit to first 20 for readability
                key = issue.get("key", "N/A")
                summary = issue.get("summary", "")[:40] + ("..." if len(issue.get("summary", "")) > 40 else "")
                status = issue.get("status", "N/A")
                age = issue.get("age_days", 0)
                assignee = issue.get("assignee") or "Unassigned"
                team_name = issue.get("team", "No Team")
                markdown_content += f"| {key} | {summary} | {status} | {age}d | {assignee} | {team_name} |\n"

            if len(detailed_issues) > 20:
                markdown_content += f"\n*... and {len(detailed_issues) - 20} more WIP issues*\n"

        # Recommendations section
        total_issues = summary.get("total_wip_issues", 0)
        issues_over_threshold = summary.get("issues_over_threshold", 0)
        avg_age = summary.get("average_age_days", 0)

        markdown_content += f"""

## AI Agent Recommendations

### Flow Health Assessment
- **Current WIP**: {total_issues} issues
- **Risk Level**: {self._get_risk_level(issues_over_threshold, total_issues)}
- **Average Age**: {avg_age:.1f} days ({"Above Threshold" if avg_age > alert_threshold else "Within Threshold"})

### Next Actions
"""

        # Generate contextual recommendations
        if issues_over_threshold > 0:
            percentage = (issues_over_threshold / total_issues * 100) if total_issues > 0 else 0
            if percentage > 30:
                markdown_content += (
                    "1. **CRITICAL**: High percentage of aging issues - investigate flow blockers immediately\n"
                )
                markdown_content += "2. **URGENT**: Review and prioritize issues over threshold for completion\n"
                markdown_content += "3. **ACTION**: Consider reducing WIP limits to improve flow\n"
            elif percentage > 15:
                markdown_content += "1. **MONITOR**: Moderate aging detected - review bottlenecks in workflow\n"
                markdown_content += "2. **OPTIMIZE**: Focus on completing existing work before new intake\n"
            else:
                markdown_content += "1. **ATTENTION**: Few issues aging - spot check for potential blockers\n"
        else:
            markdown_content += "1. **HEALTHY**: All WIP within aging thresholds - maintain current flow\n"

        if status_breakdown:
            max_avg_status = max(status_breakdown.items(), key=lambda x: x[1].get("avg_age_days", 0))
            if max_avg_status[1].get("avg_age_days", 0) > alert_threshold:
                markdown_content += f"4. **BOTTLENECK**: '{max_avg_status[0]}' status shows highest average age\n"

        if avg_age > alert_threshold:
            markdown_content += "5. **PROCESS**: Overall average age exceeds threshold - review workflow efficiency\n"

        markdown_content += f"""

## Data Structure (JSON)

```json
{{
    "metadata": {{
        "project_key": "{project_key}",
        "alert_threshold_days": {alert_threshold},
        "team": "{team}"
    }},
    "summary": {{
        "total_wip_issues": {summary.get("total_wip_issues", 0)},
        "issues_over_threshold": {summary.get("issues_over_threshold", 0)},
        "average_age_days": {summary.get("average_age_days", 0):.1f},
        "median_age_days": {summary.get("median_age_days", 0):.1f}
    }}
}}
```

---
*Generated by PyToolkit JIRA WIP Age Tracking Service*
"""

        return markdown_content

    def _get_alert_status_emoji(self, issues_over_threshold: int) -> str:
        """Get emoji for alert status."""
        if issues_over_threshold == 0:
            return "✅"
        elif issues_over_threshold <= 3:
            return "⚠️"
        else:
            return "🚨"

    def _get_age_status_emoji(self, age: float, threshold: int) -> str:
        """Get emoji for age status."""
        if age <= threshold * 0.7:
            return "✅"
        elif age <= threshold:
            return "⚠️"
        else:
            return "🚨"

    def _get_percentile_assessment(self, value: float, threshold: int) -> str:
        """Get assessment text for percentile values."""
        if value <= threshold:
            return "✅ Healthy"
        elif value <= threshold * 1.5:
            return "⚠️ Monitor"
        else:
            return "🚨 Action Needed"

    def _get_status_health_emoji(self, over_threshold: int, total: int) -> str:
        """Get health emoji for status breakdown."""
        if total == 0:
            return "➖"

        percentage = (over_threshold / total) * 100
        if percentage == 0:
            return "✅"
        elif percentage <= 25:
            return "⚠️"
        else:
            return "🚨"

    def _get_risk_level(self, issues_over_threshold: int, total_issues: int) -> str:
        """Get risk level assessment."""
        if total_issues == 0:
            return "No Issues"

        percentage = (issues_over_threshold / total_issues) * 100

        if percentage == 0:
            return "✅ Low Risk"
        elif percentage <= 15:
            return "⚠️ Moderate Risk"
        elif percentage <= 30:
            return "🚨 High Risk"
        else:
            return "🚨 Critical Risk"
