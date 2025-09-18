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
from domains.syngenta.jira.workflow_config_service import WorkflowConfigService
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
    cycle_time_hours: Optional[float]  # Accumulative time in WIP statuses
    lead_time_hours: Optional[float]  # Total time from first transition to Done
    has_valid_cycle_time: bool
    has_batch_update_pattern: bool  # Indicates potential batch status updates


class CycleTimeService:
    """Service class for cycle time analysis."""

    def __init__(self):
        self.jira_assistant = JiraAssistant()
        self.logger = LogManager.get_instance().get_logger("CycleTimeService")
        self.time_parser = TimePeriodParser()
        self.workflow_config = WorkflowConfigService()

        # Cache for workflow config to avoid repeated calls
        self._cached_project_key = None
        self._cached_workflow_config = None
        self._cached_wip_statuses = set()
        self._cached_done_statuses = set()
        self._cached_archived_statuses = set()

    def _is_wip_status_cached(self, status: str) -> bool:
        """Check if status is WIP using cached config."""
        return status in self._cached_wip_statuses

    def _is_done_status_cached(self, status: str) -> bool:
        """Check if status is Done using cached config."""
        return status in self._cached_done_statuses

    def _is_archived_status_cached(self, status: str) -> bool:
        """Check if status is Archived using cached config."""
        return status in self._cached_archived_statuses

    def _clear_cache(self):
        """Clear cached workflow config."""
        self._cached_project_key = None
        self._cached_workflow_config = None
        self._cached_wip_statuses = set()
        self._cached_done_statuses = set()
        self._cached_archived_statuses = set()

    def _get_cycle_time_emoji(self, cycle_time_hours: float) -> str:
        """Get emoji for cycle time performance."""
        if cycle_time_hours < 8:  # Less than 1 day
            return "🟢"  # Excellent
        elif cycle_time_hours < 24:  # Less than 1 day
            return "🟢"  # Good
        elif cycle_time_hours < 72:  # Less than 3 days
            return "🟡"  # Moderate
        elif cycle_time_hours < 168:  # Less than 1 week
            return "🟠"  # Concerning
        else:
            return "🔴"  # Poor

    def _get_performance_emoji(self, avg_cycle_time_hours: float) -> str:
        """Get emoji for overall performance based on average cycle time."""
        if avg_cycle_time_hours < 24:  # Less than 1 day
            return "🟢"  # Excellent
        elif avg_cycle_time_hours < 72:  # Less than 3 days
            return "🟡"  # Good
        elif avg_cycle_time_hours < 168:  # Less than 1 week
            return "🟠"  # Moderate
        else:
            return "🔴"  # Needs improvement

    def _get_performance_assessment(self, avg_cycle_time_hours: float) -> str:
        """Get performance assessment text based on average cycle time."""
        if avg_cycle_time_hours < 24:
            return "Excellent - Very fast resolution times"
        elif avg_cycle_time_hours < 72:
            return "Good - Reasonable resolution times"
        elif avg_cycle_time_hours < 168:
            return "Moderate - Some room for improvement"
        else:
            return "Needs Improvement - Resolution times are concerning"

    def _get_priority_emoji(self, priority: str) -> str:
        """Get emoji for priority level."""
        priority_map = {"critical": "🔥", "highest": "🔥", "high": "🔴", "medium": "🟡", "low": "🟢", "lowest": "🟢"}
        return priority_map.get(priority.lower(), "⚪") if priority else "⚪"

    def analyze_cycle_time(
        self,
        project_key: str,
        time_period: str,
        issue_types: List[str],
        team: Optional[str] = None,
        priorities: Optional[List[str]] = None,
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
            verbose (bool): Enable verbose output
            output_file (Optional[str]): Output file path

        Returns:
            Dict: Analysis results with metrics and issue details
        """
        jql_query = None  # Initialize for error handling
        try:
            self.logger.info(f"Starting cycle time analysis for project {project_key}")

            # Cache workflow config for this project to avoid repeated calls
            self._cached_project_key = project_key
            self._cached_workflow_config = self.workflow_config.get_workflow_config(project_key)
            self._cached_wip_statuses = set(self._cached_workflow_config.get("status_mapping", {}).get("wip", []))
            self._cached_done_statuses = set(self._cached_workflow_config.get("status_mapping", {}).get("done", []))
            self._cached_archived_statuses = set(
                self._cached_workflow_config.get("status_mapping", {}).get("archived", [])
            )

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
            )

            self.logger.info(f"Executing JQL query: {jql_query}")

            # Fetch issues with changelog to get status transition history
            issues = self.jira_assistant.fetch_issues(
                jql_query=jql_query,
                fields=(
                    "key,summary,issuetype,status,priority,assignee,customfield_10265"  # Squad[Dropdown] field
                ),
                max_results=100,
                expand_changelog=True,  # Important: We need changelog for status transitions
            )

            self.logger.info(f"Fetched {len(issues)} issues for analysis")

            # Analyze each issue for cycle time
            cycle_time_results = []
            issues_without_cycle_time = 0
            archived_issues_excluded = 0
            zero_cycle_time_anomalies = []

            for issue in issues:
                result = self._analyze_issue_cycle_time(issue, project_key)

                # Exclude issues that went through Archived status (11)
                if self._issue_went_through_archived(issue, project_key):
                    archived_issues_excluded += 1
                    continue

                if not result.has_valid_cycle_time:
                    issues_without_cycle_time += 1
                    continue

                # Separate zero cycle time anomalies (less than 10 minutes = 0.167 hours)
                if result.cycle_time_hours is not None and result.cycle_time_hours < 0.167:
                    zero_cycle_time_anomalies.append(result)
                else:
                    cycle_time_results.append(result)

            # Log filtering summary
            if issues_without_cycle_time > 0:
                self.logger.info(f"Excluded {issues_without_cycle_time} issues without valid cycle time data.")

            if archived_issues_excluded > 0:
                self.logger.info(f"Excluded {archived_issues_excluded} issues that went through Archived status.")

            if zero_cycle_time_anomalies:
                self.logger.info(
                    f"Found {len(zero_cycle_time_anomalies)} zero cycle time anomalies (batch updates/admin closures)."
                )

            self.logger.info(f"Analyzing {len(cycle_time_results)} issues with valid cycle time")

            # Calculate metrics
            metrics = self._calculate_metrics(cycle_time_results, zero_cycle_time_anomalies)

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
                    "analysis_date": datetime.now().isoformat(),
                },
                "metrics": metrics,
                "issues": [self._result_to_dict(result) for result in cycle_time_results],
                "anomalies": [self._result_to_dict(result) for result in zero_cycle_time_anomalies],
            }

            # Save to file if specified
            if output_file:
                self._save_results(results, output_file)
            else:
                # Generate default output file in organized structure
                output_path = OutputManager.get_output_path("cycle-time", f"cycle_time_{project_key}")
                self._save_results(results, output_path)

            # Print verbose output if requested
            if verbose:
                self._print_verbose_results(cycle_time_results, zero_cycle_time_anomalies, metrics)

            # Clear cache after analysis
            self._clear_cache()

            return results

        except Exception as e:
            # Check if this is a JQL error and provide helpful suggestions
            error_str = str(e)
            if "400" in error_str and (
                "does not exist for the field 'type'" in error_str or "invalid issue type" in error_str.lower()
            ):
                self.logger.error("JQL Query Error (400): Invalid issue type detected.")
                self.logger.error(f"Original query: {jql_query}")

                # Try to fetch available issue types for the project
                try:
                    available_types = self.jira_assistant.fetch_project_issue_types(project_key)
                    type_names = [t.get("name") for t in available_types if t.get("name")]  # Filter out None values
                    self.logger.error(f"Available issue types for project {project_key}: {type_names}")

                    # Check which provided types are not available
                    invalid_types = [t for t in issue_types if t not in type_names]
                    if invalid_types:
                        self.logger.error(f"Invalid issue types provided: {invalid_types}")
                        self.logger.error("Suggested command with valid types:")
                        valid_types = [t for t in issue_types if t in type_names]
                        # Ensure we have only string values for type names
                        safe_type_names = [name for name in type_names[:5] if name and isinstance(name, str)]
                        suggested_types = ",".join(valid_types) if valid_types else ",".join(safe_type_names)
                        self.logger.error(f"--issue-types '{suggested_types}'")
                except Exception as metadata_error:
                    self.logger.warning(f"Could not fetch project metadata: {metadata_error}")

            self.logger.error(f"Error in cycle time analysis: {e}")
            # Clear cache on error
            self._clear_cache()
            raise

    def _build_jql_query(
        self,
        project_key: str,
        start_date: datetime,
        end_date: datetime,
        issue_types: List[str],
        team: Optional[str],
        priorities: Optional[List[str]],
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
        jql_parts.append(f"resolved >= '{start_date_str}' AND resolved <= '{end_date_str}'")

        # Team filter
        if team:
            jql_parts.append(f"'Squad[Dropdown]' = '{team}'")

        # Priority filter
        if priorities:
            priorities_str = "', '".join(priorities)
            jql_parts.append(f"priority in ('{priorities_str}')")

        # Use cached Done status names for filtering resolved issues
        if self._cached_done_statuses:
            done_statuses_str = "', '".join(self._cached_done_statuses)
            jql_parts.append(f"status in ('{done_statuses_str}')")
        else:
            # Fallback to statusCategory if no done statuses configured
            jql_parts.append("statusCategory = 'Done'")

        return " AND ".join(jql_parts)

    def _analyze_issue_cycle_time(self, issue: Dict, project_key: str) -> CycleTimeResult:
        """Analyze cycle time for a single issue."""

        # Ensure cache is initialized for this project
        if self._cached_project_key != project_key:
            self._cached_project_key = project_key
            self._cached_workflow_config = self.workflow_config.get_workflow_config(project_key)
            self._cached_wip_statuses = set(self._cached_workflow_config.get("status_mapping", {}).get("wip", []))
            self._cached_done_statuses = set(self._cached_workflow_config.get("status_mapping", {}).get("done", []))
            self._cached_archived_statuses = set(
                self._cached_workflow_config.get("status_mapping", {}).get("archived", [])
            )

        fields = issue.get("fields", {})
        changelog = issue.get("changelog", {})

        # Extract issue information
        issue_key = issue.get("key", "")
        summary = fields.get("summary", "")
        issue_type = fields.get("issuetype", {}).get("name", "")
        status = fields.get("status", {}).get("name", "")
        status_category = fields.get("status", {}).get("statusCategory", {}).get("name", "")
        priority = fields.get("priority", {}).get("name") if fields.get("priority") else None
        assignee = fields.get("assignee", {}).get("displayName") if fields.get("assignee") else None
        team = fields.get("customfield_10265", {}).get("value") if fields.get("customfield_10265") else None

        # Find Started and Done timestamps from changelog
        started_date, done_date, cycle_time_hours, lead_time_hours, has_batch_pattern = (
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
            lead_time_hours=lead_time_hours,
            has_valid_cycle_time=has_valid_cycle_time,
            has_batch_update_pattern=has_batch_pattern,
        )

    def _calculate_cycle_time_from_changelog(
        self, changelog: Dict
    ) -> Tuple[Optional[str], Optional[str], Optional[float], Optional[float], bool]:
        """
        Calculate cycle time from issue changelog using sophisticated logic:
        1. Accumulative time in WIP statuses
        2. Detection of batch updates (multiple transitions in short time)
        3. First WIP entry to final Done exit

        Args:
            changelog (Dict): Issue changelog from JIRA API

        Returns:
            Tuple containing: started_date, done_date, cycle_time_hours, lead_time_hours, has_batch_pattern
        """

        # Collect all status transitions with timestamps
        transitions = []
        histories = changelog.get("histories", [])

        for history in histories:
            created = history.get("created")
            items = history.get("items", [])

            for item in items:
                if item.get("field") == "status":
                    to_status = item.get("toString")
                    if to_status:
                        transitions.append(
                            {
                                "timestamp": created,
                                "status": to_status,
                                "is_wip": self._is_wip_status_cached(to_status),
                                "is_done": self._is_done_status_cached(to_status),
                            }
                        )

        # Sort transitions by timestamp
        transitions.sort(key=lambda x: x["timestamp"])

        # Calculate accumulative cycle time approach
        started_date = None
        done_date = None
        cycle_time_hours = None
        lead_time_hours = None
        has_batch_pattern = False

        # Method 1: Accumulative time in WIP statuses
        total_wip_time = 0.0
        current_wip_start = None
        first_wip_timestamp = None
        final_done_timestamp = None
        first_transition_timestamp = None

        if transitions:
            first_transition_timestamp = transitions[0]["timestamp"]

        for i, transition in enumerate(transitions):
            transition_dt = self._parse_datetime(transition["timestamp"])

            # Track first WIP entry
            if transition["is_wip"] and first_wip_timestamp is None:
                first_wip_timestamp = transition["timestamp"]
                current_wip_start = transition_dt

            # Track final Done transition
            if transition["is_done"]:
                final_done_timestamp = transition["timestamp"]
                # If we were in WIP, add the time
                if current_wip_start is not None:
                    wip_duration = (transition_dt - current_wip_start).total_seconds() / 3600
                    total_wip_time += wip_duration
                    current_wip_start = None

            # Handle WIP to WIP transitions
            elif transition["is_wip"] and current_wip_start is not None:
                # Continue in WIP, no action needed
                pass

            # Handle non-WIP transitions (pausing work)
            elif not transition["is_wip"] and not transition["is_done"] and current_wip_start is not None:
                # We left WIP status, add accumulated time
                wip_duration = (transition_dt - current_wip_start).total_seconds() / 3600
                total_wip_time += wip_duration
                current_wip_start = None

            # Re-entering WIP after being out
            elif transition["is_wip"] and current_wip_start is None and first_wip_timestamp is not None:
                current_wip_start = transition_dt

        # If we ended in WIP status (shouldn't happen for resolved issues, but let's be safe)
        if current_wip_start is not None and final_done_timestamp is not None:
            final_done_dt = self._parse_datetime(final_done_timestamp)
            if final_done_dt > current_wip_start:
                wip_duration = (final_done_dt - current_wip_start).total_seconds() / 3600
                total_wip_time += wip_duration

        # Calculate lead time (total time from first transition to Done)
        if first_transition_timestamp and final_done_timestamp:
            first_dt = self._parse_datetime(first_transition_timestamp)
            final_dt = self._parse_datetime(final_done_timestamp)
            lead_time_delta = final_dt - first_dt
            lead_time_hours = lead_time_delta.total_seconds() / 3600

        # Set results based on accumulative calculation
        if first_wip_timestamp and final_done_timestamp:
            started_date = first_wip_timestamp
            done_date = final_done_timestamp
            cycle_time_hours = total_wip_time

            # Detect potential batch updates (multiple transitions within 5 minutes)
            if len(transitions) >= 3:
                batch_window_minutes = 5
                rapid_transitions = 0
                for i in range(1, len(transitions)):
                    current_dt = self._parse_datetime(transitions[i]["timestamp"])
                    prev_dt = self._parse_datetime(transitions[i - 1]["timestamp"])
                    time_diff_minutes = (current_dt - prev_dt).total_seconds() / 60
                    if time_diff_minutes <= batch_window_minutes:
                        rapid_transitions += 1

                # If more than 50% of transitions are rapid, mark as potential batch update
                if rapid_transitions / len(transitions) > 0.5:
                    has_batch_pattern = True
                    self.logger.debug(
                        f"Potential batch update detected: {rapid_transitions}/{len(transitions)} rapid transitions"
                    )

        elif final_done_timestamp and not first_wip_timestamp:
            # Issue was resolved without going through WIP
            cycle_time_hours = 0.0
            done_date = final_done_timestamp
            started_date = final_done_timestamp
            self.logger.debug(f"Issue resolved without WIP transition: {final_done_timestamp}")

        return started_date, done_date, cycle_time_hours, lead_time_hours, has_batch_pattern

    def _issue_went_through_archived(self, issue: Dict, project_key: str) -> bool:
        """Check if an issue went through Archived status using cached config."""

        # Ensure cache is initialized for this project
        if self._cached_project_key != project_key:
            self._cached_project_key = project_key
            self._cached_workflow_config = self.workflow_config.get_workflow_config(project_key)
            self._cached_wip_statuses = set(self._cached_workflow_config.get("status_mapping", {}).get("wip", []))
            self._cached_done_statuses = set(self._cached_workflow_config.get("status_mapping", {}).get("done", []))
            self._cached_archived_statuses = set(
                self._cached_workflow_config.get("status_mapping", {}).get("archived", [])
            )

        changelog = issue.get("changelog", {})
        histories = changelog.get("histories", [])

        for history in histories:
            items = history.get("items", [])
            for item in items:
                if item.get("field") == "status":
                    to_status = item.get("toString", "")
                    # Check if it's an archived status using cached config
                    if self._is_archived_status_cached(to_status):
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
                return datetime.fromisoformat(date_string.replace("Z", "+00:00")).replace(tzinfo=None)
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
                    return datetime.fromisoformat(date_string_fixed).replace(tzinfo=None)
                else:
                    # Already has colon or other format
                    return datetime.fromisoformat(date_string).replace(tzinfo=None)
            else:
                # No timezone info: 2025-07-18T18:43:31.570
                return datetime.fromisoformat(date_string)
        except ValueError as e:
            self.logger.error(f"Failed to parse datetime string '{date_string}': {e}")
            raise ValueError(f"Invalid datetime format: {date_string}")

    def _calculate_metrics(
        self, results: List[CycleTimeResult], anomalies: Optional[List[CycleTimeResult]] = None
    ) -> Dict:
        """Calculate cycle time metrics."""

        if anomalies is None:
            anomalies = []

        total_issues = len(results) + len(anomalies)
        valid_cycle_time_issues = len(results)
        anomaly_count = len(anomalies)

        if valid_cycle_time_issues == 0:
            return {
                "total_issues": total_issues,
                "issues_with_valid_cycle_time": 0,
                "zero_cycle_time_anomalies": anomaly_count,
                "anomaly_percentage": (anomaly_count / total_issues * 100) if total_issues > 0 else 0,
                "average_cycle_time_hours": 0.0,
                "median_cycle_time_hours": 0.0,
                "min_cycle_time_hours": 0.0,
                "max_cycle_time_hours": 0.0,
                "average_lead_time_hours": 0.0,
                "median_lead_time_hours": 0.0,
                "min_lead_time_hours": 0.0,
                "max_lead_time_hours": 0.0,
                "time_distribution": {},
                "anomaly_analysis": self._analyze_anomalies(anomalies),
            }

        # Extract cycle times from valid results only
        cycle_times = [result.cycle_time_hours for result in results if result.cycle_time_hours is not None]

        if len(cycle_times) == 0:
            return {
                "total_issues": total_issues,
                "issues_with_valid_cycle_time": 0,
                "zero_cycle_time_anomalies": anomaly_count,
                "anomaly_percentage": (anomaly_count / total_issues * 100) if total_issues > 0 else 0,
                "average_cycle_time_hours": 0.0,
                "median_cycle_time_hours": 0.0,
                "min_cycle_time_hours": 0.0,
                "max_cycle_time_hours": 0.0,
                "average_lead_time_hours": 0.0,
                "median_lead_time_hours": 0.0,
                "min_lead_time_hours": 0.0,
                "max_lead_time_hours": 0.0,
                "time_distribution": {},
                "anomaly_analysis": self._analyze_anomalies(anomalies),
            }

        # Calculate statistical metrics
        average_cycle_time = statistics.mean(cycle_times)
        median_cycle_time = statistics.median(cycle_times)
        min_cycle_time = min(cycle_times)
        max_cycle_time = max(cycle_times)

        # Extract lead times from valid results
        lead_times = [result.lead_time_hours for result in results if result.lead_time_hours is not None]

        # Calculate lead time metrics
        average_lead_time = statistics.mean(lead_times) if lead_times else 0.0
        median_lead_time = statistics.median(lead_times) if lead_times else 0.0
        min_lead_time = min(lead_times) if lead_times else 0.0
        max_lead_time = max(lead_times) if lead_times else 0.0

        # Calculate time distribution
        time_distribution = self._calculate_time_distribution(cycle_times)

        # Calculate priority breakdown
        priority_breakdown = self._calculate_priority_breakdown(results)

        return {
            "total_issues": total_issues,
            "issues_with_valid_cycle_time": valid_cycle_time_issues,
            "zero_cycle_time_anomalies": anomaly_count,
            "anomaly_percentage": (anomaly_count / total_issues * 100) if total_issues > 0 else 0,
            "average_cycle_time_hours": average_cycle_time,
            "median_cycle_time_hours": median_cycle_time,
            "min_cycle_time_hours": min_cycle_time,
            "max_cycle_time_hours": max_cycle_time,
            "average_lead_time_hours": average_lead_time,
            "median_lead_time_hours": median_lead_time,
            "min_lead_time_hours": min_lead_time,
            "max_lead_time_hours": max_lead_time,
            "time_distribution": time_distribution,
            "priority_breakdown": priority_breakdown,
            "anomaly_analysis": self._analyze_anomalies(anomalies),
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

    def _calculate_priority_breakdown(self, results: List[CycleTimeResult]) -> Dict[str, Dict]:
        """Calculate cycle time metrics grouped by priority."""

        priority_groups = {}

        # Group results by priority
        for result in results:
            if result.cycle_time_hours is not None:
                priority = result.priority or "No Priority"
                if priority not in priority_groups:
                    priority_groups[priority] = {"cycle_times": [], "lead_times": []}
                priority_groups[priority]["cycle_times"].append(result.cycle_time_hours)
                # Also collect lead times for the same issues
                if result.lead_time_hours is not None:
                    priority_groups[priority]["lead_times"].append(result.lead_time_hours)

        # Calculate metrics for each priority
        priority_metrics = {}
        for priority, times_data in priority_groups.items():
            cycle_times = times_data["cycle_times"]
            lead_times = times_data["lead_times"]

            if cycle_times:
                metrics = {
                    "count": len(cycle_times),
                    "average_cycle_time_hours": statistics.mean(cycle_times),
                    "median_cycle_time_hours": statistics.median(cycle_times),
                    "min_cycle_time_hours": min(cycle_times),
                    "max_cycle_time_hours": max(cycle_times),
                }

                # Add lead time metrics if available
                if lead_times:
                    metrics.update(
                        {
                            "average_lead_time_hours": statistics.mean(lead_times),
                            "median_lead_time_hours": statistics.median(lead_times),
                            "min_lead_time_hours": min(lead_times),
                            "max_lead_time_hours": max(lead_times),
                        }
                    )
                else:
                    # If no lead times available, set to 0
                    metrics.update(
                        {
                            "average_lead_time_hours": 0.0,
                            "median_lead_time_hours": 0.0,
                            "min_lead_time_hours": 0.0,
                            "max_lead_time_hours": 0.0,
                        }
                    )

                priority_metrics[priority] = metrics

        return priority_metrics

    def _analyze_anomalies(self, anomalies: List[CycleTimeResult]) -> Dict:
        """Analyze zero cycle time anomalies and provide insights."""
        if not anomalies:
            return {"count": 0, "percentage": 0.0, "patterns": {}, "lead_time_stats": {}, "recommendations": []}

        # Count batch update patterns
        batch_update_count = sum(1 for a in anomalies if a.has_batch_update_pattern)

        # Analyze lead times for anomalies
        lead_times = [a.lead_time_hours for a in anomalies if a.lead_time_hours is not None and a.lead_time_hours > 0]

        lead_time_stats = {}
        if lead_times:
            lead_time_stats = {
                "count": len(lead_times),
                "average_lead_time_hours": statistics.mean(lead_times),
                "median_lead_time_hours": statistics.median(lead_times),
                "min_lead_time_hours": min(lead_times),
                "max_lead_time_hours": max(lead_times),
            }

        # Analyze patterns
        patterns = {
            "batch_updates": batch_update_count,
            "administrative_closures": len(anomalies) - batch_update_count,
            "batch_update_percentage": (batch_update_count / len(anomalies) * 100) if anomalies else 0,
        }

        # Generate recommendations
        recommendations = []
        if batch_update_count > 0:
            recommendations.append("Consider process training to avoid batch status updates")
        if batch_update_count / len(anomalies) > 0.5:
            recommendations.append("High percentage of batch updates detected - review workflow practices")
        if lead_times and statistics.mean(lead_times) > 72:  # > 3 days
            recommendations.append("Anomalies show high lead times - investigate delayed administrative processes")

        return {
            "count": len(anomalies),
            "patterns": patterns,
            "lead_time_stats": lead_time_stats,
            "recommendations": recommendations,
        }

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
            "cycle_time_days": (result.cycle_time_hours / 24 if result.cycle_time_hours else None),
            "lead_time_hours": result.lead_time_hours,
            "lead_time_days": (result.lead_time_hours / 24 if result.lead_time_hours else None),
            "has_valid_cycle_time": result.has_valid_cycle_time,
            "has_batch_update_pattern": result.has_batch_update_pattern,
        }

    def _format_as_markdown(self, results: Dict) -> str:
        """
        Format cycle time analysis results as markdown optimized for AI consumption.

        Args:
            results (Dict): Analysis results containing metadata, metrics, and issues

        Returns:
            str: Markdown formatted report
        """
        metadata = results.get("analysis_metadata", {})
        metrics = results.get("metrics", {})
        issues = results.get("issues", [])

        # Extract metadata
        project_key = metadata.get("project_key", "Unknown")
        time_period = metadata.get("time_period", "Unknown")
        start_date = metadata.get("start_date", "")
        end_date = metadata.get("end_date", "")
        issue_types = metadata.get("issue_types", [])
        team = metadata.get("team")
        priorities = metadata.get("priorities", [])
        analysis_date = metadata.get("analysis_date", "")

        # Calculate performance assessment
        avg_cycle_time = metrics.get("average_cycle_time_hours", 0)
        performance_emoji = self._get_performance_emoji(avg_cycle_time)
        performance_assessment = self._get_performance_assessment(avg_cycle_time)

        md_content = []

        # Header
        md_content.append(f"# {performance_emoji} JIRA Cycle Time Analysis Report")
        md_content.append("")
        md_content.append(f"**Project:** {project_key}")
        md_content.append(f"**Analysis Period:** {time_period}")
        if start_date and end_date:
            md_content.append(f"**Date Range:** {start_date[:10]} to {end_date[:10]}")
        md_content.append(f"**Issue Types:** {', '.join(issue_types)}")
        if team:
            md_content.append(f"**Team Filter:** {team}")
        if priorities:
            md_content.append(f"**Priority Filter:** {', '.join(priorities)}")
        md_content.append(f"**Generated:** {analysis_date[:19] if analysis_date else 'Unknown'}")
        md_content.append("")

        # Executive Summary
        md_content.append("## ⏱️ Executive Summary")
        md_content.append("")
        avg_days = avg_cycle_time / 24
        md_content.append(f"**Average Cycle Time:** {avg_cycle_time:.1f} hours ({avg_days:.1f} days)")
        md_content.append(f"**Performance Assessment:** {performance_assessment}")
        md_content.append("")
        md_content.append(f"- **Total Issues Analyzed:** {metrics.get('total_issues', 0)}")
        md_content.append(f"- **Issues with Valid Cycle Time:** {metrics.get('issues_with_valid_cycle_time', 0)}")

        median_cycle_time = metrics.get("median_cycle_time_hours", 0)
        median_days = median_cycle_time / 24
        min_cycle_time = metrics.get("min_cycle_time_hours", 0)
        min_days = min_cycle_time / 24
        max_cycle_time = metrics.get("max_cycle_time_hours", 0)
        max_days = max_cycle_time / 24

        md_content.append(f"- **Median Cycle Time:** {median_cycle_time:.1f} hours ({median_days:.1f} days)")
        md_content.append(f"- **Fastest Resolution:** {min_cycle_time:.1f} hours ({min_days:.1f} days)")
        md_content.append(f"- **Slowest Resolution:** {max_cycle_time:.1f} hours ({max_days:.1f} days)")
        md_content.append("")

        # Cycle Time Distribution
        time_distribution = metrics.get("time_distribution", {})
        if time_distribution:
            md_content.append("## 📊 Cycle Time Distribution")
            md_content.append("")
            md_content.append("| Time Range | Count | Percentage | Performance |")
            md_content.append("|-------------|-------|------------|-------------|")

            total_with_cycle_time = metrics.get("issues_with_valid_cycle_time", 1)
            if total_with_cycle_time == 0:
                total_with_cycle_time = 1  # Avoid division by zero

            for time_range, count in time_distribution.items():
                if count > 0:
                    percentage = (count / total_with_cycle_time) * 100

                    # Determine performance emoji based on time range
                    if "< 4 hours" in time_range or "4-8 hours" in time_range:
                        perf_emoji = "🟢"
                    elif "8-24 hours" in time_range or "1-3 days" in time_range:
                        perf_emoji = "🟡"
                    elif "3-7 days" in time_range:
                        perf_emoji = "🟠"
                    else:
                        perf_emoji = "🔴"

                    md_content.append(f"| {time_range} | {count} | {percentage:.1f}% | {perf_emoji} |")

            md_content.append("")

        # Priority Analysis
        priority_breakdown = metrics.get("priority_breakdown", {})
        if priority_breakdown:
            md_content.append("## 🎯 Priority-Based Analysis")
            md_content.append("")
            md_content.append("| Priority | Count | Avg Cycle Time | Median | Min | Max | Emoji |")
            md_content.append("|----------|-------|----------------|---------|-----|-----|-------|")

            # Sort priorities by average cycle time (fastest first)
            sorted_priorities = sorted(
                priority_breakdown.items(), key=lambda x: x[1].get("average_cycle_time_hours", 0)
            )

            for priority, p_metrics in sorted_priorities:
                count = p_metrics.get("count", 0)
                avg_hours = p_metrics.get("average_cycle_time_hours", 0)
                avg_days = avg_hours / 24
                median_hours = p_metrics.get("median_cycle_time_hours", 0)
                median_days = median_hours / 24
                min_hours = p_metrics.get("min_cycle_time_hours", 0)
                min_days = min_hours / 24
                max_hours = p_metrics.get("max_cycle_time_hours", 0)
                max_days = max_hours / 24

                priority_emoji = self._get_priority_emoji(priority)
                perf_emoji = self._get_performance_emoji(avg_hours)

                md_content.append(
                    f"| {priority} {priority_emoji} | {count} | {avg_hours:.1f}h ({avg_days:.1f}d) | {median_hours:.1f}h ({median_days:.1f}d) | {min_hours:.1f}h | {max_hours:.1f}h | {perf_emoji} |"
                )

            md_content.append("")

        # Performance Analysis
        md_content.append("## 📈 Performance Analysis")
        md_content.append("")

        if avg_cycle_time < 24:
            md_content.append("### ✅ Excellent Performance")
            md_content.append("- Very fast resolution times indicate efficient processes")
            md_content.append("- Team is effectively managing workload and complexity")
            md_content.append("")
        elif avg_cycle_time < 72:
            md_content.append("### 🟡 Good Performance")
            md_content.append("- Reasonable resolution times with room for optimization")
            md_content.append("- Consider reviewing processes for potential improvements")
            md_content.append("")
        elif avg_cycle_time < 168:
            md_content.append("### 🟠 Moderate Performance")
            md_content.append("- Resolution times indicate some process bottlenecks")
            md_content.append("- Focus on identifying and addressing delays")
            md_content.append("")
        else:
            md_content.append("### 🔴 Performance Concerns")
            md_content.append("- Long resolution times suggest significant process issues")
            md_content.append("- Immediate attention needed to improve efficiency")
            md_content.append("")

        # Detailed Issue Analysis
        if issues:
            md_content.append("## 📝 Detailed Issue Analysis")
            md_content.append("")

            # Filter and sort issues by cycle time
            valid_issues = [issue for issue in issues if issue.get("has_valid_cycle_time", False)]
            sorted_issues = sorted(valid_issues, key=lambda x: x.get("cycle_time_hours", 0))

            if len(sorted_issues) <= 20:  # Show all if 20 or fewer
                md_content.append("### All Issues with Cycle Time Data")
                md_content.append("")
                md_content.append("| Issue Key | Summary | Type | Priority | Cycle Time | Performance | Assignee |")
                md_content.append("|-----------|---------|------|----------|------------|-------------|----------|")

                for issue in sorted_issues:
                    issue_key = issue.get("issue_key", "N/A")
                    summary = issue.get("summary", "N/A")[:40] + ("..." if len(issue.get("summary", "")) > 40 else "")
                    issue_type = issue.get("issue_type", "N/A")
                    priority = issue.get("priority", "No Priority")
                    cycle_time_hours = issue.get("cycle_time_hours", 0)
                    cycle_time_days = issue.get("cycle_time_days", 0)
                    assignee = issue.get("assignee", "Unassigned")

                    priority_emoji = self._get_priority_emoji(priority)
                    perf_emoji = self._get_cycle_time_emoji(cycle_time_hours)

                    md_content.append(
                        f"| {issue_key} | {summary} | {issue_type} | {priority} {priority_emoji} | {cycle_time_hours:.1f}h ({cycle_time_days:.1f}d) | {perf_emoji} | {assignee} |"
                    )
            else:
                # Show top performers and concerning issues
                md_content.append("### Top Performers (Fastest 10)")
                md_content.append("")
                md_content.append("| Issue Key | Summary | Type | Cycle Time | Assignee |")
                md_content.append("|-----------|---------|------|------------|----------|")

                for issue in sorted_issues[:10]:
                    issue_key = issue.get("issue_key", "N/A")
                    summary = issue.get("summary", "N/A")[:40] + ("..." if len(issue.get("summary", "")) > 40 else "")
                    issue_type = issue.get("issue_type", "N/A")
                    cycle_time_hours = issue.get("cycle_time_hours", 0)
                    cycle_time_days = issue.get("cycle_time_days", 0)
                    assignee = issue.get("assignee", "Unassigned")

                    md_content.append(
                        f"| {issue_key} | {summary} | {issue_type} | {cycle_time_hours:.1f}h ({cycle_time_days:.1f}d) | {assignee} |"
                    )

                md_content.append("")
                md_content.append("### Issues Needing Attention (Slowest 10)")
                md_content.append("")
                md_content.append("| Issue Key | Summary | Type | Cycle Time | Assignee |")
                md_content.append("|-----------|---------|------|------------|----------|")

                for issue in sorted_issues[-10:]:
                    issue_key = issue.get("issue_key", "N/A")
                    summary = issue.get("summary", "N/A")[:40] + ("..." if len(issue.get("summary", "")) > 40 else "")
                    issue_type = issue.get("issue_type", "N/A")
                    cycle_time_hours = issue.get("cycle_time_hours", 0)
                    cycle_time_days = issue.get("cycle_time_days", 0)
                    assignee = issue.get("assignee", "Unassigned")

                    md_content.append(
                        f"| {issue_key} | {summary} | {issue_type} | {cycle_time_hours:.1f}h ({cycle_time_days:.1f}d) | {assignee} |"
                    )

            md_content.append("")

        # Recommendations
        md_content.append("## 💡 Recommendations")
        md_content.append("")

        if avg_cycle_time < 24:
            md_content.append("### ✅ Maintain Excellence")
            md_content.append("- Continue current efficient practices")
            md_content.append("- Share successful processes with other teams")
            md_content.append("- Monitor for any performance degradation")
            md_content.append("")
        elif avg_cycle_time < 72:
            md_content.append("### 🔍 Optimization Opportunities")
            md_content.append("- Analyze top-performing issues for best practices")
            md_content.append("- Identify and eliminate minor bottlenecks")
            md_content.append("- Standardize successful resolution patterns")
            md_content.append("")
        elif avg_cycle_time < 168:
            md_content.append("### ⚠️ Process Improvement Needed")
            md_content.append("- Conduct detailed analysis of slow-resolving issues")
            md_content.append("- Implement more frequent status check-ins")
            md_content.append("- Review workload distribution and capacity planning")
            md_content.append("- Consider breaking down complex issues")
            md_content.append("")
        else:
            md_content.append("### 🚨 Immediate Action Required")
            md_content.append("- Urgently review and redesign resolution processes")
            md_content.append("- Implement daily standups for issue tracking")
            md_content.append("- Consider resource reallocation or additional training")
            md_content.append("- Establish escalation procedures for stuck issues")
            md_content.append("")

        # Data Quality Notes
        md_content.append("## 📋 Data Quality Notes")
        md_content.append("")
        md_content.append("- Analysis based on issues resolved within the specified time period")
        md_content.append("- Cycle time calculated from first 'Started' to final 'Done' status transition")
        md_content.append("- Issues that went through 'Archived' status are excluded from analysis")
        md_content.append("- Only issues with both Start and Done timestamps are included in calculations")
        md_content.append("- Times are calculated in hours and converted to days for readability")
        md_content.append("")

        # Trending Analysis Section
        trending_analysis = results.get("trending_analysis")
        if trending_analysis:
            md_content.append("## 📈 Trending Analysis")
            md_content.append("")

            # Baseline Period
            baseline_period = trending_analysis.get("baseline_period", {})
            if baseline_period:
                baseline_start = baseline_period.get("start", "")[:10]  # YYYY-MM-DD
                baseline_end = baseline_period.get("end", "")[:10]
                md_content.append(f"**Baseline Period:** {baseline_start} to {baseline_end}")
                md_content.append("")

            # Trend Metrics
            trend_metrics = trending_analysis.get("trend_metrics", [])
            if trend_metrics:
                md_content.append("### 🔄 Trend Metrics")
                md_content.append("")
                md_content.append("| Metric | Current | Baseline | Change | Direction | Significance |")
                md_content.append("|--------|---------|----------|--------|-----------|--------------|")

                for trend in trend_metrics:
                    metric_name = trend.get("metric_name", "")
                    current_value = trend.get("current_value", 0)
                    baseline_value = trend.get("baseline_value", 0)
                    change_percent = trend.get("change_percent", 0)
                    trend_direction = trend.get("trend_direction", "")
                    significance = trend.get("significance", False)

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
                    direction_emoji = (
                        "📈" if trend_direction == "IMPROVING" else "📉" if trend_direction == "DEGRADING" else "➡️"
                    )
                    significance_emoji = "✅" if significance else "➖"

                    md_content.append(
                        f"| {metric_name} | {current_str} | {baseline_str} | {change_str} | {direction_emoji} {trend_direction} | {significance_emoji} |"
                    )

                md_content.append("")

            # Alerts
            alerts = trending_analysis.get("alerts", [])
            if alerts:
                md_content.append("### 🚨 Trend Alerts")
                md_content.append("")

                # Group alerts by severity
                critical_alerts = [a for a in alerts if a.get("severity") == "CRITICAL"]
                warning_alerts = [a for a in alerts if a.get("severity") == "WARNING"]
                info_alerts = [a for a in alerts if a.get("severity") == "INFO"]

                if critical_alerts:
                    md_content.append("#### 🔴 Critical Alerts")
                    for alert in critical_alerts:
                        md_content.append(f"- **{alert.get('metric', '')}**: {alert.get('message', '')}")
                        md_content.append(f"  - *Recommendation*: {alert.get('recommendation', '')}")
                    md_content.append("")

                if warning_alerts:
                    md_content.append("#### 🟡 Warning Alerts")
                    for alert in warning_alerts:
                        md_content.append(f"- **{alert.get('metric', '')}**: {alert.get('message', '')}")
                        md_content.append(f"  - *Recommendation*: {alert.get('recommendation', '')}")
                    md_content.append("")

                if info_alerts:
                    md_content.append("#### ℹ️ Info Alerts")
                    for alert in info_alerts:
                        md_content.append(f"- **{alert.get('metric', '')}**: {alert.get('message', '')}")
                        md_content.append(f"  - *Recommendation*: {alert.get('recommendation', '')}")
                    md_content.append("")
            else:
                md_content.append("### ✅ No Alerts")
                md_content.append("All metrics are within normal ranges - no trending concerns detected.")
                md_content.append("")

        return "\n".join(md_content)

    def _save_results(self, results: Dict, output_file: str):
        """Save results to JSON file."""
        try:
            JSONManager.write_json(results, output_file)
            self.logger.info(f"Results saved to {output_file}")
        except Exception as e:
            self.logger.error(f"Failed to save results to {output_file}: {e}")
            raise

    def _print_verbose_results(self, results: List[CycleTimeResult], anomalies: List[CycleTimeResult], metrics: Dict):
        """Print verbose results to console."""
        print("\n" + "=" * 80)
        print("DETAILED CYCLE TIME ANALYSIS")
        print("=" * 80)

        # Print summary statistics
        total_issues = len(results) + len(anomalies)
        if total_issues > 0:
            anomaly_percentage = (len(anomalies) / total_issues) * 100
            print("\n📊 SUMMARY:")
            print(f"✅ With Valid Cycle Time: {len(results)}")
            print(f"🚨 Zero Cycle Time Anomalies: {len(anomalies)} ({anomaly_percentage:.1f}%)")

        # Print anomalies first
        if anomalies:
            print(f"\n🚨 ZERO CYCLE TIME ANOMALIES ({len(anomalies)}):")
            print("-" * 60)
            print("These issues show 0.0h cycle time, indicating batch updates or administrative closures.")
            print("Lead time is used as alternative metric for these cases.\n")

            for result in anomalies[:10]:  # Show first 10 anomalies
                lead_time_days = result.lead_time_hours / 24 if result.lead_time_hours else 0
                batch_indicator = " 🔄 [BATCH UPDATE]" if result.has_batch_update_pattern else " 📋 [ADMIN CLOSURE]"

                print(f"  {result.issue_key}: {result.summary}{batch_indicator}")
                print(f"    Type: {result.issue_type} | Priority: {result.priority or 'Not set'}")
                if result.lead_time_hours:
                    print(f"    Lead Time: {result.lead_time_hours:.1f} hours ({lead_time_days:.1f} days)")
                print(f"    Assignee: {result.assignee or 'Unassigned'}")
                print()

            if len(anomalies) > 10:
                print(f"  ... and {len(anomalies) - 10} more anomalies")
            print()

        # Sort valid results by cycle time (fastest to slowest)
        sorted_results = sorted(
            [r for r in results if r.has_valid_cycle_time],
            key=lambda x: x.cycle_time_hours or 0,
        )

        if sorted_results:
            print(f"\n✅ ISSUES WITH VALID CYCLE TIME DATA ({len(sorted_results)}):")
            print("-" * 60)

            for result in sorted_results:
                cycle_time_days = result.cycle_time_hours / 24 if result.cycle_time_hours else 0
                lead_time_days = result.lead_time_hours / 24 if result.lead_time_hours else 0
                batch_indicator = " 🔄 [BATCH UPDATE]" if result.has_batch_update_pattern else ""

                print(f"  {result.issue_key}: {result.summary}{batch_indicator}")
                print(
                    f"    Type: {result.issue_type} | Status: {result.status} | Priority: {result.priority or 'Not set'}"
                )
                print(f"    Started: {result.started_date}")
                print(f"    Done: {result.done_date}")
                print(f"    Cycle Time (WIP): {result.cycle_time_hours:.1f} hours ({cycle_time_days:.1f} days)")
                if result.lead_time_hours:
                    print(f"    Lead Time (Total): {result.lead_time_hours:.1f} hours ({lead_time_days:.1f} days)")
                print(f"    Assignee: {result.assignee or 'Unassigned'}")
                print(f"    Team: {result.team or 'Not set'}")
                if result.has_batch_update_pattern:
                    print("    ⚠️  Detected batch status updates - actual work time may be longer")
                print()

        # Print anomaly analysis
        anomaly_analysis = metrics.get("anomaly_analysis", {})
        if anomaly_analysis.get("count", 0) > 0:
            print("\n📊 ANOMALY ANALYSIS:")
            print("-" * 40)
            patterns = anomaly_analysis.get("patterns", {})
            print(f"Batch Updates: {patterns.get('batch_updates', 0)}")
            print(f"Administrative Closures: {patterns.get('administrative_closures', 0)}")
            print(f"Batch Update Rate: {patterns.get('batch_update_percentage', 0):.1f}%")

            lead_stats = anomaly_analysis.get("lead_time_stats", {})
            if lead_stats:
                avg_lead = lead_stats.get("average_lead_time_hours", 0)
                print(f"Average Lead Time: {avg_lead:.1f} hours ({avg_lead / 24:.1f} days)")

            recommendations = anomaly_analysis.get("recommendations", [])
            if recommendations:
                print("\n💡 Recommendations:")
                for rec in recommendations:
                    print(f"  • {rec}")

        print("=" * 80)
