"""
JIRA Cycle Time Service

This service provides functionality to analyze cycle time by calculating the time taken
for tickets to move from Started status (07) to Done status (10), excluding Archived
tickets (11).
"""

import statistics
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from domains.syngenta.jira.issue_adherence_service import TimePeriodParser
from utils.data.json_manager import JSONManager
from utils.jira.jira_assistant import JiraAssistant
from utils.logging.logging_manager import LogManager
from utils.output_manager import OutputManager


@dataclass
class CycleTimeResult:
    """Data class to hold cycle time analysis results."""

    issue_key: str
    summary: str
    issue_type: str
    status: str
    status_category: str
    priority: Optional[str]
    assignee: Optional[str]
    team: Optional[str]
    started_date: Optional[str]
    done_date: Optional[str]
    cycle_time_hours: Optional[float]  # Time from Started to Done in hours
    has_valid_cycle_time: bool


class CycleTimeService:
    """Service class for cycle time analysis."""

    def __init__(self):
        self.jira_assistant = JiraAssistant()
        self.logger = LogManager.get_instance().get_logger("CycleTimeService")
        self.time_parser = TimePeriodParser()

    def analyze_cycle_time(
        self,
        project_key: str,
        time_period: str,
        issue_types: List[str],
        team: Optional[str] = None,
        priorities: Optional[List[str]] = None,
        status_categories: Optional[List[str]] = None,
        verbose: bool = False,
        output_file: Optional[str] = None,
    ) -> Dict:
        """
        Analyze cycle time for issues that were resolved within a specified time period.

        Args:
            project_key (str): JIRA project key
            time_period (str): Time period to analyze
            issue_types (List[str]): List of issue types to include
            team (Optional[str]): Team name to filter by
            priorities (Optional[List[str]]): List of priorities to filter by
            status_categories (List[str]): Status categories to include
            verbose (bool): Enable verbose output
            output_file (Optional[str]): Output file path

        Returns:
            Dict: Analysis results with metrics and issue details
        """
        try:
            self.logger.info(f"Starting cycle time analysis for project {project_key}")

            # Parse time period
            start_date, end_date = self.time_parser.parse_time_period(time_period)

            # Build JQL query
            jql_query = self._build_jql_query(
                project_key,
                start_date,
                end_date,
                issue_types,
                team,
                priorities,
                status_categories,
            )

            self.logger.info(f"Executing JQL query: {jql_query}")

            # Fetch issues with changelog to get status transition history
            issues = self.jira_assistant.fetch_issues(
                jql_query=jql_query,
                fields=(
                    "key,summary,issuetype,status,priority,assignee,"
                    "customfield_10265"  # Squad[Dropdown] field
                ),
                max_results=100,
                expand_changelog=True,  # Important: We need changelog for status transitions
            )

            self.logger.info(f"Fetched {len(issues)} issues for analysis")

            # Analyze each issue for cycle time
            cycle_time_results = []
            issues_without_cycle_time = 0
            archived_issues_excluded = 0

            for issue in issues:
                result = self._analyze_issue_cycle_time(issue)

                # Exclude issues that went through Archived status (11)
                if self._issue_went_through_archived(issue):
                    archived_issues_excluded += 1
                    continue

                if not result.has_valid_cycle_time:
                    issues_without_cycle_time += 1
                    continue

                cycle_time_results.append(result)

            # Log filtering summary
            if issues_without_cycle_time > 0:
                self.logger.info(
                    f"Excluded {issues_without_cycle_time} issues without valid cycle time data."
                )

            if archived_issues_excluded > 0:
                self.logger.info(
                    f"Excluded {archived_issues_excluded} issues that went through Archived status."
                )

            self.logger.info(
                f"Analyzing {len(cycle_time_results)} issues with valid cycle time"
            )

            # Calculate metrics
            metrics = self._calculate_metrics(cycle_time_results)

            # Prepare results
            results = {
                "analysis_metadata": {
                    "project_key": project_key,
                    "time_period": time_period,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "issue_types": issue_types,
                    "team": team,
                    "priorities": priorities,
                    "status_categories": status_categories,
                    "analysis_date": datetime.now().isoformat(),
                },
                "metrics": metrics,
                "issues": [
                    self._result_to_dict(result) for result in cycle_time_results
                ],
            }

            # Save to file if specified
            if output_file:
                self._save_results(results, output_file)
            else:
                # Generate default output file in organized structure
                output_path = OutputManager.get_output_path(
                    "cycle-time", f"cycle_time_{project_key}"
                )
                self._save_results(results, output_path)

            # Print verbose output if requested
            if verbose:
                self._print_verbose_results(cycle_time_results, metrics)

            return results

        except Exception as e:
            # Check if this is a JQL error and provide helpful suggestions
            error_str = str(e)
            if "400" in error_str and (
                "does not exist for the field 'type'" in error_str
                or "invalid issue type" in error_str.lower()
            ):
                self.logger.error(
                    "JQL Query Error (400): Invalid issue type detected."
                )
                self.logger.error(f"Original query: {jql_query}")

                # Try to fetch available issue types for the project
                try:
                    available_types = self.jira_assistant.fetch_project_issue_types(
                        project_key
                    )
                    type_names = [t.get("name") for t in available_types]
                    self.logger.error(
                        f"Available issue types for project {project_key}: {type_names}"
                    )

                    # Check which provided types are not available
                    invalid_types = [t for t in issue_types if t not in type_names]
                    if invalid_types:
                        self.logger.error(
                            f"Invalid issue types provided: {invalid_types}"
                        )
                        self.logger.error("Suggested command with valid types:")
                        valid_types = [t for t in issue_types if t in type_names]
                        suggested_types = (
                            ",".join(valid_types)
                            if valid_types
                            else ",".join(type_names[:5])
                        )
                        self.logger.error(f"--issue-types '{suggested_types}'")
                except Exception as metadata_error:
                    self.logger.warning(
                        f"Could not fetch project metadata: {metadata_error}"
                    )

            self.logger.error(f"Error in cycle time analysis: {e}")
            raise

    def _build_jql_query(
        self,
        project_key: str,
        start_date: datetime,
        end_date: datetime,
        issue_types: List[str],
        team: Optional[str],
        priorities: Optional[List[str]],
        status_categories: Optional[List[str]],
    ) -> str:
        """Build JQL query for fetching issues."""

        # Base query
        jql_parts = [f"project = '{project_key}'"]

        # Issue types
        if issue_types:
            types_str = "', '".join(issue_types)
            jql_parts.append(f"type in ('{types_str}')")

        # Time period - using resolution date to capture issues resolved in the period
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        jql_parts.append(
            f"resolved >= '{start_date_str}' AND resolved <= '{end_date_str}'"
        )

        # Team filter
        if team:
            jql_parts.append(f"'Squad[Dropdown]' = '{team}'")

        # Priority filter
        if priorities:
            priorities_str = "', '".join(priorities)
            jql_parts.append(f"priority in ('{priorities_str}')")

        # Status categories (default to Done to get completed issues)
        if status_categories:
            categories_str = "', '".join(status_categories)
            jql_parts.append(f"statusCategory in ('{categories_str}')")

        return " AND ".join(jql_parts)

    def _analyze_issue_cycle_time(self, issue: Dict) -> CycleTimeResult:
        """Analyze cycle time for a single issue."""

        fields = issue.get("fields", {})
        changelog = issue.get("changelog", {})

        # Extract issue information
        issue_key = issue.get("key", "")
        summary = fields.get("summary", "")
        issue_type = fields.get("issuetype", {}).get("name", "")
        status = fields.get("status", {}).get("name", "")
        status_category = (
            fields.get("status", {}).get("statusCategory", {}).get("name", "")
        )
        priority = (
            fields.get("priority", {}).get("name") if fields.get("priority") else None
        )
        assignee = (
            fields.get("assignee", {}).get("displayName")
            if fields.get("assignee")
            else None
        )
        team = (
            fields.get("customfield_10265", {}).get("value")
            if fields.get("customfield_10265")
            else None
        )

        # Find Started and Done timestamps from changelog
        started_date, done_date, cycle_time_hours = (
            self._calculate_cycle_time_from_changelog(changelog)
        )

        has_valid_cycle_time = started_date is not None and done_date is not None

        return CycleTimeResult(
            issue_key=issue_key,
            summary=summary,
            issue_type=issue_type,
            status=status,
            status_category=status_category,
            priority=priority,
            assignee=assignee,
            team=team,
            started_date=started_date,
            done_date=done_date,
            cycle_time_hours=cycle_time_hours,
            has_valid_cycle_time=has_valid_cycle_time,
        )

    def _calculate_cycle_time_from_changelog(
        self, changelog: Dict
    ) -> Tuple[Optional[str], Optional[str], Optional[float]]:
        """
        Calculate cycle time from issue changelog by finding transitions to Started and Done statuses.

        Args:
            changelog (Dict): Issue changelog from JIRA API

        Returns:
            Tuple containing: started_date, done_date, cycle_time_hours
        """

        started_date = None
        done_date = None

        histories = changelog.get("histories", [])

        for history in histories:
            created = history.get("created")
            items = history.get("items", [])

            for item in items:
                if item.get("field") == "status":
                    to_status = item.get("toString", "")

                    # Look for transition to Started status (status ID 07 or status name containing "Started")
                    if self._is_started_status(to_status) and started_date is None:
                        started_date = created

                    # Look for transition to Done status (status ID 10 or status name containing "Done")
                    elif self._is_done_status(to_status):
                        done_date = created

        # Calculate cycle time if both dates are available
        cycle_time_hours = None
        if started_date and done_date:
            try:
                started_dt = self._parse_datetime(started_date)
                done_dt = self._parse_datetime(done_date)

                # Ensure done_date is after started_date
                if done_dt > started_dt:
                    cycle_time_delta = done_dt - started_dt
                    cycle_time_hours = (
                        cycle_time_delta.total_seconds() / 3600
                    )  # Convert to hours
                else:
                    # If done date is before started date, there might have been multiple transitions
                    # In this case, we should look for the last Started transition before the Done transition
                    cycle_time_hours = None

            except Exception as e:
                self.logger.warning(f"Failed to calculate cycle time: {e}")
                cycle_time_hours = None

        return started_date, done_date, cycle_time_hours

    def _is_started_status(self, status_name: str) -> bool:
        """Check if a status represents the 'Started' state."""
        started_indicators = [
            "started",
            "in progress",
            "in development",
            "doing",
            "active",
            "development",
            "coding",
            "implementing",
        ]
        status_lower = status_name.lower()
        return any(indicator in status_lower for indicator in started_indicators)

    def _is_done_status(self, status_name: str) -> bool:
        """Check if a status represents the 'Done' state."""
        done_indicators = [
            "done",
            "closed",
            "resolved",
            "completed",
            "finished",
            "delivered",
            "deployed",
            "live",
        ]
        status_lower = status_name.lower()
        return any(indicator in status_lower for indicator in done_indicators)

    def _issue_went_through_archived(self, issue: Dict) -> bool:
        """Check if an issue went through Archived status (11)."""
        changelog = issue.get("changelog", {})
        histories = changelog.get("histories", [])

        for history in histories:
            items = history.get("items", [])
            for item in items:
                if item.get("field") == "status":
                    to_status = item.get("toString", "")
                    if self._is_archived_status(to_status):
                        return True
        return False

    def _is_archived_status(self, status_name: str) -> bool:
        """Check if a status represents the 'Archived' state."""
        archived_indicators = ["archived", "cancelled", "rejected", "obsolete"]
        status_lower = status_name.lower()
        return any(indicator in status_lower for indicator in archived_indicators)

    def _parse_datetime(self, date_string: str) -> datetime:
        """
        Parse datetime string from JIRA, handling various timezone formats.

        Args:
            date_string (str): DateTime string from JIRA API

        Returns:
            datetime: Parsed datetime object (timezone-naive)
        """
        try:
            # Handle different timezone formats from JIRA
            if date_string.endswith("Z"):
                # UTC format: 2025-07-18T18:43:31.570Z
                return datetime.fromisoformat(
                    date_string.replace("Z", "+00:00")
                ).replace(tzinfo=None)
            elif "+" in date_string or date_string.count("-") > 2:
                # Timezone offset format: 2025-07-18T18:43:31.570-0300 or +0000
                # Need to add colon to make it ISO compatible: -0300 -> -03:00
                import re

                # Find timezone offset pattern and add colon if missing
                tz_pattern = r"([+-]\d{4})$"
                match = re.search(tz_pattern, date_string)
                if match:
                    tz_offset = match.group(1)
                    # Convert -0300 to -03:00 format
                    tz_formatted = f"{tz_offset[:3]}:{tz_offset[3:]}"
                    date_string_fixed = date_string.replace(tz_offset, tz_formatted)
                    return datetime.fromisoformat(date_string_fixed).replace(
                        tzinfo=None
                    )
                else:
                    # Already has colon or other format
                    return datetime.fromisoformat(date_string).replace(tzinfo=None)
            else:
                # No timezone info: 2025-07-18T18:43:31.570
                return datetime.fromisoformat(date_string)
        except ValueError as e:
            self.logger.error(f"Failed to parse datetime string '{date_string}': {e}")
            raise ValueError(f"Invalid datetime format: {date_string}")

    def _calculate_metrics(self, results: List[CycleTimeResult]) -> Dict:
        """Calculate cycle time metrics."""

        total_issues = len(results)

        if total_issues == 0:
            return {
                "total_issues": 0,
                "issues_with_cycle_time": 0,
                "average_cycle_time_hours": 0.0,
                "median_cycle_time_hours": 0.0,
                "min_cycle_time_hours": 0.0,
                "max_cycle_time_hours": 0.0,
                "time_distribution": {},
            }

        # Extract cycle times
        cycle_times = [
            result.cycle_time_hours
            for result in results
            if result.cycle_time_hours is not None
        ]
        issues_with_cycle_time = len(cycle_times)

        if issues_with_cycle_time == 0:
            return {
                "total_issues": total_issues,
                "issues_with_cycle_time": 0,
                "average_cycle_time_hours": 0.0,
                "median_cycle_time_hours": 0.0,
                "min_cycle_time_hours": 0.0,
                "max_cycle_time_hours": 0.0,
                "time_distribution": {},
            }

        # Calculate statistical metrics
        average_cycle_time = statistics.mean(cycle_times)
        median_cycle_time = statistics.median(cycle_times)
        min_cycle_time = min(cycle_times)
        max_cycle_time = max(cycle_times)

        # Calculate time distribution
        time_distribution = self._calculate_time_distribution(cycle_times)

        # Calculate priority breakdown
        priority_breakdown = self._calculate_priority_breakdown(results)

        return {
            "total_issues": total_issues,
            "issues_with_cycle_time": issues_with_cycle_time,
            "average_cycle_time_hours": average_cycle_time,
            "median_cycle_time_hours": median_cycle_time,
            "min_cycle_time_hours": min_cycle_time,
            "max_cycle_time_hours": max_cycle_time,
            "time_distribution": time_distribution,
            "priority_breakdown": priority_breakdown,
        }

    def _calculate_time_distribution(self, cycle_times: List[float]) -> Dict[str, int]:
        """Calculate distribution of cycle times by time ranges."""

        distribution = {
            "< 4 hours": 0,
            "4-8 hours": 0,
            "8-24 hours (1 day)": 0,
            "1-3 days": 0,
            "3-7 days": 0,
            "1-2 weeks": 0,
            "> 2 weeks": 0,
        }

        for cycle_time in cycle_times:
            if cycle_time < 4:
                distribution["< 4 hours"] += 1
            elif cycle_time < 8:
                distribution["4-8 hours"] += 1
            elif cycle_time < 24:
                distribution["8-24 hours (1 day)"] += 1
            elif cycle_time < 72:  # 3 days
                distribution["1-3 days"] += 1
            elif cycle_time < 168:  # 7 days
                distribution["3-7 days"] += 1
            elif cycle_time < 336:  # 14 days
                distribution["1-2 weeks"] += 1
            else:
                distribution["> 2 weeks"] += 1

        return distribution

    def _calculate_priority_breakdown(
        self, results: List[CycleTimeResult]
    ) -> Dict[str, Dict]:
        """Calculate cycle time metrics grouped by priority."""

        priority_groups = {}

        # Group results by priority
        for result in results:
            if result.cycle_time_hours is not None:
                priority = result.priority or "No Priority"
                if priority not in priority_groups:
                    priority_groups[priority] = []
                priority_groups[priority].append(result.cycle_time_hours)

        # Calculate metrics for each priority
        priority_metrics = {}
        for priority, cycle_times in priority_groups.items():
            if cycle_times:
                priority_metrics[priority] = {
                    "count": len(cycle_times),
                    "average_cycle_time_hours": statistics.mean(cycle_times),
                    "median_cycle_time_hours": statistics.median(cycle_times),
                    "min_cycle_time_hours": min(cycle_times),
                    "max_cycle_time_hours": max(cycle_times),
                }

        return priority_metrics

    def _result_to_dict(self, result: CycleTimeResult) -> Dict:
        """Convert result to dictionary."""
        return {
            "issue_key": result.issue_key,
            "summary": result.summary,
            "issue_type": result.issue_type,
            "status": result.status,
            "status_category": result.status_category,
            "priority": result.priority,
            "assignee": result.assignee,
            "team": result.team,
            "started_date": result.started_date,
            "done_date": result.done_date,
            "cycle_time_hours": result.cycle_time_hours,
            "cycle_time_days": (
                result.cycle_time_hours / 24 if result.cycle_time_hours else None
            ),
            "has_valid_cycle_time": result.has_valid_cycle_time,
        }

    def _save_results(self, results: Dict, output_file: str):
        """Save results to JSON file."""
        try:
            JSONManager.write_json(results, output_file)
            self.logger.info(f"Results saved to {output_file}")
        except Exception as e:
            self.logger.error(f"Failed to save results to {output_file}: {e}")
            raise

    def _print_verbose_results(self, results: List[CycleTimeResult], metrics: Dict):
        """Print verbose results to console."""
        print("\n" + "=" * 80)
        print("DETAILED CYCLE TIME ANALYSIS")
        print("=" * 80)

        # Sort by cycle time (fastest to slowest)
        sorted_results = sorted(
            [r for r in results if r.has_valid_cycle_time],
            key=lambda x: x.cycle_time_hours or 0,
        )

        print(f"\nISSUES WITH CYCLE TIME DATA ({len(sorted_results)}):")
        print("-" * 60)

        for result in sorted_results:
            cycle_time_days = (
                result.cycle_time_hours / 24 if result.cycle_time_hours else 0
            )
            print(f"  {result.issue_key}: {result.summary}")
            print(
                f"    Type: {result.issue_type} | Status: {result.status} | Priority: {result.priority or 'Not set'}"
            )
            print(f"    Started: {result.started_date}")
            print(f"    Done: {result.done_date}")
            print(
                f"    Cycle Time: {result.cycle_time_hours:.1f} hours ({cycle_time_days:.1f} days)"
            )
            print(f"    Assignee: {result.assignee or 'Unassigned'}")
            print(f"    Team: {result.team or 'Not set'}")
            print()

        print("=" * 80)
