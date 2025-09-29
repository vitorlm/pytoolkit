"""
JIRA Issue Velocity Analysis Service

This service analyzes issue velocity by tracking both creation and resolution rates
over time, providing insights into team productivity and backlog trends.

Key functionalities:
- Fetch issues created and resolved within specified time periods
- Calculate monthly/quarterly velocity metrics
- Analyze team productivity trends and backlog impact  
- Support flexible filtering by team, issue types, and labels
- Export results to JSON/CSV formats
"""

import hashlib
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from domains.syngenta.jira.jira_processor import JiraProcessor
from utils.cache_manager.cache_manager import CacheManager
from utils.data.json_manager import JSONManager
from utils.logging.logging_manager import LogManager
from utils.output_manager import OutputManager


class IssueVelocityService:
    """Service for analyzing JIRA issue velocity through creation vs resolution tracking."""

    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("IssueVelocityService")
        self.jira_processor = JiraProcessor()
        self.cache_manager = CacheManager.get_instance()

    def clear_cache(self):
        """Clear all cached data."""
        self.cache_manager.clear_all()
        self.logger.info("Velocity analysis cache cleared")

    def analyze_issue_velocity(
        self,
        project_key: str,
        time_period: str,
        issue_types: List[str] = ["Bug"],
        teams: Optional[List[str]] = None,
        labels: Optional[List[str]] = None,
        aggregation: str = "monthly",
        include_summary: bool = False,
        verbose: bool = False,
        output_file: Optional[str] = None,
        export: Optional[str] = None,
    ) -> Dict:
        """
        Analyze issue velocity by comparing creation vs resolution rates.

        Args:
            project_key: JIRA project key
            time_period: Time period for analysis
            issue_types: List of issue types to analyze
            teams: Team/squad filter (one or more names)
            labels: List of labels to filter
            aggregation: monthly or quarterly
            include_summary: Include detailed summary statistics
            verbose: Enable verbose output
            output_file: Custom output file path
            export: Export format (json/csv)

        Returns:
            Dictionary containing velocity analysis results
        """
        try:
            self.logger.info(f"Starting velocity analysis for project {project_key}")

            # Parse time period
            start_date, end_date = self._parse_time_period(time_period)
            self.logger.info(f"Analyzing period: {start_date} to {end_date}")

            # Build filters
            filters = {
                "project_key": project_key,
                "start_date": start_date,
                "end_date": end_date,
                "issue_types": issue_types,
                "teams": teams,
                "labels": labels,
            }

            # Generate cache key
            cache_key = self._generate_cache_key("velocity", filters, aggregation)

            # Try to load from cache
            cached_result = self.cache_manager.load(cache_key, expiration_minutes=60)
            if cached_result is not None:
                self.logger.info("Using cached velocity data")
                return self._finalize_result(cached_result, output_file, export, include_summary)

            # Fetch created and resolved issues
            self.logger.info("Fetching created issues...")
            created_issues = self._fetch_created_issues(filters)
            self.logger.info(f"Fetched {len(created_issues)} created issues")

            self.logger.info("Fetching resolved issues...")
            resolved_issues = self._fetch_resolved_issues(filters)
            self.logger.info(f"Fetched {len(resolved_issues)} resolved issues")

            self.logger.info(
                f"Total: {len(created_issues)} created, {len(resolved_issues)} resolved issues"
            )

            # Group issues by time periods
            periods = self._get_time_periods(start_date, end_date, aggregation)

            # Calculate velocity metrics
            velocity_data = self._calculate_velocity_metrics(
                created_issues, resolved_issues, periods, issue_types
            )

            # Calculate summary statistics
            summary_stats = None
            if include_summary:
                summary_stats = self._calculate_summary_statistics(velocity_data, aggregation)

            # Build result
            # Prepare team label for display
            team_label = None
            if teams:
                seen = set()
                ordered = []
                for t in teams:
                    if t and t not in seen:
                        seen.add(t)
                        ordered.append(t)
                team_label = ", ".join(ordered) if ordered else None

            result = {
                "analysis_metadata": {
                    "project_key": project_key,
                    "time_period": time_period,
                    "period_display": f"{start_date} to {end_date}",
                    "issue_types": issue_types,
                    "team": team_label,
                    "teams": teams,
                    "labels": labels,
                    "aggregation": aggregation,
                    "generated_at": datetime.now().isoformat(),
                },
                "velocity_data": velocity_data,
                "summary_statistics": summary_stats,
                "by_issue_type": self._calculate_breakdown_by_type(
                    created_issues, resolved_issues, issue_types
                ),
            }

            # Add verbose data if requested
            if verbose:
                result["created_issues_detail"] = created_issues
                result["resolved_issues_detail"] = resolved_issues

            # Cache the result
            self.cache_manager.save(cache_key, result)

            return self._finalize_result(result, output_file, export, include_summary)

        except Exception as e:
            self.logger.error(f"Velocity analysis failed: {e}", exc_info=True)
            raise

    def _parse_time_period(self, time_period: str) -> Tuple[datetime, datetime]:
        """
        Parse time period string into start and end dates.

        Args:
            time_period: Time period string

        Returns:
            Tuple of (start_date, end_date)
        """
        try:
            if time_period.startswith("last-"):
                return self._parse_relative_period(time_period)
            elif " to " in time_period:
                return self._parse_date_range(time_period)
            else:
                raise ValueError(f"Invalid time period format: {time_period}")
        except Exception as e:
            raise ValueError(f"Invalid time period format: {time_period}. {str(e)}")

    def _parse_relative_period(self, time_period: str) -> Tuple[datetime, datetime]:
        """Parse relative time periods like 'last-6-months'."""
        now = datetime.now()

        if time_period == "last-6-months":
            start_date = now - timedelta(days=180)
        elif time_period == "last-year":
            start_date = now - timedelta(days=365)
        elif time_period == "last-2-years":
            start_date = now - timedelta(days=730)
        else:
            raise ValueError(f"Unsupported relative period: {time_period}")

        return start_date, now

    def _parse_date_range(self, time_period: str) -> Tuple[datetime, datetime]:
        """Parse date range like '2024-01-01 to 2024-12-31'."""
        try:
            start_str, end_str = time_period.split(" to ")
            start_date = datetime.strptime(start_str.strip(), "%Y-%m-%d")
            end_date = datetime.strptime(end_str.strip(), "%Y-%m-%d")

            # Set end date to end of day
            end_date = end_date.replace(hour=23, minute=59, second=59)

            return start_date, end_date
        except ValueError:
            raise ValueError("Invalid date range format. Use: YYYY-MM-DD to YYYY-MM-DD")

    def _fetch_created_issues(self, filters: Dict) -> List[Dict]:
        """Fetch issues created within the specified time period."""
        jql = self._build_created_issues_jql(filters)
        self.logger.debug(f"Created issues JQL: {jql}")

        issues = self.jira_processor.jira_assistant.fetch_issues(
            jql_query=jql,
            fields="key,summary,issuetype,status,priority,assignee,created,resolutiondate,labels,customfield_10851",
            max_results=100  # Use 100 to work with JIRA limitation on custom fields
        )
        return self._process_issues_data(issues, "created")

    def _fetch_resolved_issues(self, filters: Dict) -> List[Dict]:
        """Fetch issues resolved within the specified time period."""
        jql = self._build_resolved_issues_jql(filters)
        self.logger.debug(f"Resolved issues JQL: {jql}")

        issues = self.jira_processor.jira_assistant.fetch_issues(
            jql_query=jql,
            fields="key,summary,issuetype,status,priority,assignee,created,resolutiondate,labels,customfield_10851", 
            max_results=100  # Use 100 to work with JIRA limitation on custom fields
        )
        return self._process_issues_data(issues, "resolved")

    def _build_created_issues_jql(self, filters: Dict) -> str:
        """Build JQL query for created issues."""
        jql_parts = [f"project = {filters['project_key']}"]

        # Date filter for creation
        start_date = filters["start_date"].strftime("%Y-%m-%d")
        end_date = filters["end_date"].strftime("%Y-%m-%d")
        jql_parts.append(f"created >= '{start_date}'")
        jql_parts.append(f"created <= '{end_date}'")

        # Issue types filter
        if filters.get("issue_types"):
            types_str = "','".join(filters["issue_types"])
            jql_parts.append(f"type IN ('{types_str}')")

        # Team filter
        teams = filters.get("teams")
        if teams:
            cleaned = []
            seen = set()
            for t in teams:
                if t:
                    t = t.strip()
                    if t and t not in seen:
                        seen.add(t)
                        cleaned.append(t)
            if len(cleaned) == 1:
                jql_parts.append(f"'Squad[Dropdown]' = '{cleaned[0]}'")
            elif len(cleaned) > 1:
                vals = "', '".join(cleaned)
                jql_parts.append(f"'Squad[Dropdown]' in ('{vals}')")

        # Labels filter
        if filters.get("labels"):
            for label in filters["labels"]:
                jql_parts.append(f"labels = '{label}'")

        jql = " AND ".join(jql_parts) + " ORDER BY created ASC"
        return jql

    def _build_resolved_issues_jql(self, filters: Dict) -> str:
        """Build JQL query for resolved issues."""
        jql_parts = [f"project = {filters['project_key']}"]

        # Date filter for resolution
        start_date = filters["start_date"].strftime("%Y-%m-%d")
        end_date = filters["end_date"].strftime("%Y-%m-%d")
        jql_parts.append(f"resolutiondate >= '{start_date}'")
        jql_parts.append(f"resolutiondate <= '{end_date}'")

        # Only resolved issues
        jql_parts.append("statusCategory = Done")

        # Issue types filter
        if filters.get("issue_types"):
            types_str = "','".join(filters["issue_types"])
            jql_parts.append(f"type IN ('{types_str}')")

        # Team filter
        teams = filters.get("teams")
        if teams:
            cleaned = []
            seen = set()
            for t in teams:
                if t:
                    t = t.strip()
                    if t and t not in seen:
                        seen.add(t)
                        cleaned.append(t)
            if len(cleaned) == 1:
                jql_parts.append(f"'Squad[Dropdown]' = '{cleaned[0]}'")
            elif len(cleaned) > 1:
                vals = "', '".join(cleaned)
                jql_parts.append(f"'Squad[Dropdown]' in ('{vals}')")

        # Labels filter
        if filters.get("labels"):
            for label in filters["labels"]:
                jql_parts.append(f"labels = '{label}'")

        jql = " AND ".join(jql_parts) + " ORDER BY resolutiondate ASC"
        return jql

    def _process_issues_data(self, issues: List[Dict], _date_type: str) -> List[Dict]:
        """Process and normalize issues data."""
        processed_issues = []

        for issue in issues:
            try:
                issue_data = {
                    "key": issue.get("key"),
                    "summary": issue.get("fields", {}).get("summary"),
                    "issue_type": issue.get("fields", {}).get("issuetype", {}).get("name"),
                    "status": issue.get("fields", {}).get("status", {}).get("name"),
                    "priority": (
                        issue.get("fields", {}).get("priority", {}).get("name")
                        if issue.get("fields", {}).get("priority")
                        else "None"
                    ),
                    "assignee": self._get_assignee_name(issue.get("fields", {}).get("assignee")),
                    "created": issue.get("fields", {}).get("created"),
                    "resolved": issue.get("fields", {}).get("resolutiondate"),
                    "labels": issue.get("fields", {}).get("labels", []),
                }

                # Add team info if available
                squad_field = issue.get("fields", {}).get("customfield_10851")  # Squad[Dropdown]
                if squad_field:
                    issue_data["team"] = squad_field.get("value", "Unknown")

                processed_issues.append(issue_data)

            except Exception as e:
                self.logger.warning(f"Error processing issue {issue.get('key', 'unknown')}: {e}")
                continue

        return processed_issues

    def _get_assignee_name(self, assignee_data):
        """Extract assignee name from JIRA assignee data."""
        if not assignee_data:
            return "Unassigned"
        return assignee_data.get("displayName", assignee_data.get("name", "Unknown"))

    def _get_time_periods(
        self, start_date: datetime, end_date: datetime, aggregation: str
    ) -> List[Dict]:
        """Generate list of time periods for analysis."""
        periods = []
        current_date = start_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        while current_date <= end_date:
            if aggregation == "monthly":
                # Calculate month end
                if current_date.month == 12:
                    next_month = current_date.replace(year=current_date.year + 1, month=1)
                else:
                    next_month = current_date.replace(month=current_date.month + 1)

                period_end = next_month - timedelta(days=1)
                period_end = period_end.replace(hour=23, minute=59, second=59)

                periods.append(
                    {
                        "period": current_date.strftime("%Y-%m"),
                        "start": current_date,
                        "end": min(period_end, end_date),
                        "display": current_date.strftime("%Y-%m"),
                    }
                )

                current_date = next_month

            elif aggregation == "quarterly":
                # Calculate quarter end
                quarter = ((current_date.month - 1) // 3) + 1
                quarter_start_month = (quarter - 1) * 3 + 1
                quarter_end_month = quarter * 3

                quarter_start = current_date.replace(month=quarter_start_month, day=1)

                if quarter_end_month == 12:
                    quarter_end = current_date.replace(
                        month=12, day=31, hour=23, minute=59, second=59
                    )
                else:
                    next_quarter_start = current_date.replace(month=quarter_end_month + 1, day=1)
                    quarter_end = next_quarter_start - timedelta(days=1)
                    quarter_end = quarter_end.replace(hour=23, minute=59, second=59)

                periods.append(
                    {
                        "period": f"{current_date.year}-Q{quarter}",
                        "start": quarter_start,
                        "end": min(quarter_end, end_date),
                        "display": f"{current_date.year} Q{quarter}",
                    }
                )

                # Move to next quarter
                if quarter == 4:
                    current_date = current_date.replace(year=current_date.year + 1, month=1)
                else:
                    current_date = current_date.replace(month=quarter_end_month + 1)

        return periods

    def _calculate_velocity_metrics(
        self,
        created_issues: List[Dict],
        resolved_issues: List[Dict],
        periods: List[Dict],
        issue_types: List[str],
    ) -> List[Dict]:
        """Calculate velocity metrics for each time period."""
        velocity_data = []

        for period_info in periods:
            period_start = period_info["start"]
            period_end = period_info["end"]

            # Count created issues in this period
            created_count = self._count_issues_in_period(
                created_issues, period_start, period_end, "created"
            )

            # Count resolved issues in this period
            resolved_count = self._count_issues_in_period(
                resolved_issues, period_start, period_end, "resolved"
            )

            # Calculate metrics
            net_velocity = resolved_count - created_count
            efficiency = (resolved_count / created_count * 100) if created_count > 0 else 0

            velocity_data.append(
                {
                    "period": period_info["display"],
                    "period_key": period_info["period"],
                    "start_date": period_start.isoformat(),
                    "end_date": period_end.isoformat(),
                    "created": created_count,
                    "resolved": resolved_count,
                    "net_velocity": net_velocity,
                    "efficiency_percentage": efficiency,
                    "by_issue_type": self._calculate_period_breakdown_by_type(
                        created_issues, resolved_issues, period_start, period_end, issue_types
                    ),
                }
            )

        return velocity_data

    def _count_issues_in_period(
        self, issues: List[Dict], period_start: datetime, period_end: datetime, date_field: str
    ) -> int:
        """Count issues within a specific time period."""
        count = 0

        for issue in issues:
            issue_date_str = issue.get(date_field)
            if not issue_date_str:
                continue

            try:
                # Parse JIRA datetime format
                issue_date = datetime.fromisoformat(issue_date_str.replace("Z", "+00:00"))
                # Convert to local time (remove timezone for comparison)
                issue_date = issue_date.replace(tzinfo=None)

                if period_start <= issue_date <= period_end:
                    count += 1
            except Exception as e:
                self.logger.warning(f"Error parsing date {issue_date_str}: {e}")
                continue

        return count

    def _calculate_period_breakdown_by_type(
        self,
        created_issues: List[Dict],
        resolved_issues: List[Dict],
        period_start: datetime,
        period_end: datetime,
        issue_types: List[str],
    ) -> Dict:
        """Calculate breakdown by issue type for a specific period."""
        breakdown = {}

        for issue_type in issue_types:
            created_count = self._count_issues_by_type_in_period(
                created_issues, period_start, period_end, "created", issue_type
            )
            resolved_count = self._count_issues_by_type_in_period(
                resolved_issues, period_start, period_end, "resolved", issue_type
            )

            breakdown[issue_type] = {
                "created": created_count,
                "resolved": resolved_count,
                "net": resolved_count - created_count,
            }

        return breakdown

    def _count_issues_by_type_in_period(
        self,
        issues: List[Dict],
        period_start: datetime,
        period_end: datetime,
        date_field: str,
        issue_type: str,
    ) -> int:
        """Count issues of specific type within a time period."""
        count = 0

        for issue in issues:
            if issue.get("issue_type") != issue_type:
                continue

            issue_date_str = issue.get(date_field)
            if not issue_date_str:
                continue

            try:
                issue_date = datetime.fromisoformat(issue_date_str.replace("Z", "+00:00"))
                issue_date = issue_date.replace(tzinfo=None)

                if period_start <= issue_date <= period_end:
                    count += 1
            except Exception as e:
                self.logger.warning(f"Error parsing date {issue_date_str}: {e}")
                continue

        return count

    def _calculate_breakdown_by_type(
        self, created_issues: List[Dict], resolved_issues: List[Dict], issue_types: List[str]
    ) -> Dict:
        """Calculate overall breakdown by issue type."""
        breakdown = {}

        for issue_type in issue_types:
            created_count = len([i for i in created_issues if i.get("issue_type") == issue_type])
            resolved_count = len([i for i in resolved_issues if i.get("issue_type") == issue_type])

            breakdown[issue_type] = {
                "total_created": created_count,
                "total_resolved": resolved_count,
                "net_velocity": resolved_count - created_count,
            }

        return breakdown

    def _calculate_summary_statistics(self, velocity_data: List[Dict], _aggregation: str) -> Dict:
        """Calculate summary statistics from velocity data."""
        if not velocity_data:
            return {}

        total_created = sum(period["created"] for period in velocity_data)
        total_resolved = sum(period["resolved"] for period in velocity_data)
        net_velocity = total_resolved - total_created

        # Calculate averages
        avg_created = total_created / len(velocity_data) if velocity_data else 0
        avg_resolved = total_resolved / len(velocity_data) if velocity_data else 0
        overall_efficiency = (total_resolved / total_created * 100) if total_created > 0 else 0

        # Find best and worst periods
        best_period = max(velocity_data, key=lambda x: x["efficiency_percentage"])
        worst_period = min(velocity_data, key=lambda x: x["efficiency_percentage"])

        # Calculate trend
        if len(velocity_data) >= 2:
            first_half_avg = sum(
                p["efficiency_percentage"] for p in velocity_data[: len(velocity_data) // 2]
            ) / (len(velocity_data) // 2)
            second_half_avg = sum(
                p["efficiency_percentage"] for p in velocity_data[len(velocity_data) // 2 :]
            ) / (len(velocity_data) - len(velocity_data) // 2)

            if second_half_avg > first_half_avg + 5:
                trend = "improving"
            elif second_half_avg < first_half_avg - 5:
                trend = "declining"
            else:
                trend = "stable"
        else:
            trend = "insufficient_data"

        return {
            "total_created": total_created,
            "total_resolved": total_resolved,
            "net_velocity": net_velocity,
            "average_created": avg_created,
            "average_resolved": avg_resolved,
            "overall_efficiency": overall_efficiency,
            "best_period": {
                "period": best_period["period"],
                "efficiency": best_period["efficiency_percentage"],
            },
            "worst_period": {
                "period": worst_period["period"],
                "efficiency": worst_period["efficiency_percentage"],
            },
            "trend_direction": trend,
            "backlog_change": net_velocity,
            "periods_analyzed": len(velocity_data),
        }

    def _generate_cache_key(self, prefix: str, filters: Dict, aggregation: str) -> str:
        """Generate unique cache key for the analysis."""
        key_data = {"prefix": prefix, "filters": filters, "aggregation": aggregation}
        key_string = json.dumps(key_data, sort_keys=True, default=str)
        key_hash = hashlib.md5(key_string.encode()).hexdigest()
        return f"{prefix}_{filters['project_key']}_{key_hash}"

    def _finalize_result(
        self,
        result: Dict,
        output_file: Optional[str],
        export: Optional[str],
        _include_summary: bool,
    ) -> Dict:
        """Finalize result with export and output file handling."""
        try:
            # Determine output file if not provided
            if export and not output_file:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                project_key = result.get("analysis_metadata", {}).get("project_key", "unknown")
                extension = "csv" if export == "csv" else "json"
                output_file = OutputManager.get_output_path(
                    "issue-velocity", f"velocity_{project_key}_{timestamp}.{extension}"
                )
            elif not output_file and not export:
                # Always create a JSON output file for reference
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                project_key = result.get("analysis_metadata", {}).get("project_key", "unknown")
                output_file = OutputManager.get_output_path(
                    "issue-velocity", f"velocity_{project_key}_{timestamp}.json"
                )
                export = "json"

            # Export the result
            if output_file:
                if export == "csv" or (not export and output_file.endswith(".csv")):
                    self._export_to_csv(result, output_file)
                else:
                    self._export_to_json(result, output_file)

                result["output_file"] = output_file

            return result

        except Exception as e:
            self.logger.error(f"Error finalizing result: {e}")
            return result

    def _export_to_json(self, result: Dict, output_file: str):
        """Export results to JSON file."""
        JSONManager.write_json(result, output_file)
        self.logger.info(f"Results exported to JSON: {output_file}")

    def _export_to_csv(self, result: Dict, output_file: str):
        """Export results to CSV file."""
        try:
            import csv

            velocity_data = result.get("velocity_data", [])
            if not velocity_data:
                self.logger.warning("No velocity data to export to CSV")
                return

            # Prepare CSV headers
            headers = ["period", "created", "resolved", "net_velocity", "efficiency_percentage"]

            # Add issue type columns if multiple types
            issue_types = result.get("analysis_metadata", {}).get("issue_types", [])
            if len(issue_types) > 1:
                for issue_type in issue_types:
                    headers.extend(
                        [f"{issue_type.lower()}_created", f"{issue_type.lower()}_resolved"]
                    )

            # Write CSV
            with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()

                for period_data in velocity_data:
                    row = {
                        "period": period_data["period"],
                        "created": period_data["created"],
                        "resolved": period_data["resolved"],
                        "net_velocity": period_data["net_velocity"],
                        "efficiency_percentage": round(period_data["efficiency_percentage"], 2),
                    }

                    # Add issue type breakdown if available
                    if len(issue_types) > 1:
                        by_type = period_data.get("by_issue_type", {})
                        for issue_type in issue_types:
                            type_data = by_type.get(issue_type, {})
                            row[f"{issue_type.lower()}_created"] = type_data.get("created", 0)
                            row[f"{issue_type.lower()}_resolved"] = type_data.get("resolved", 0)

                    writer.writerow(row)

            self.logger.info(f"Results exported to CSV: {output_file}")

        except Exception as e:
            self.logger.error(f"Error exporting to CSV: {e}")
            # Fallback to JSON
            json_file = output_file.replace(".csv", ".json")
            self._export_to_json(result, json_file)
            result["output_file"] = json_file
