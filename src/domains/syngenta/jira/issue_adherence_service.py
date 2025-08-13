"""
JIRA Issue Adherence Service

This service provides functionality to analyze issue adherence by checking completion
status against due dates for issues that were resolved within specified time periods.
"""

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from utils.data.json_manager import JSONManager
from utils.jira.jira_assistant import JiraAssistant
from utils.logging.logging_manager import LogManager
from utils.output_manager import OutputManager


@dataclass
class IssueAdherenceResult:
    """Data class to hold issue adherence analysis results."""

    issue_key: str
    summary: str
    issue_type: str
    status: str
    status_category: str
    due_date: Optional[str]
    resolution_date: Optional[str]
    assignee: Optional[str]
    team: Optional[str]
    adherence_status: str  # 'early', 'on_time', 'late', 'overdue', 'no_due_date'
    days_difference: Optional[
        int
    ]  # positive = late, negative = early, None = no due date or not resolved


class TimePeriodParser:
    """Utility class to parse time period strings."""

    @staticmethod
    def parse_time_period(time_period: str) -> Tuple[datetime, datetime]:
        """
        Parse time period string and return start and end dates.

        Args:
            time_period (str): Time period string. Supports:
                - Relative periods: 'last-week', 'last-2-weeks', 'last-month', 'N-days'
                - Date ranges: 'YYYY-MM-DD to YYYY-MM-DD' (e.g., '2025-06-09 to 2025-06-22')
                - Single dates: 'YYYY-MM-DD' (treats as single day range)

        Returns:
            Tuple[datetime, datetime]: Start and end dates
        """
        # Check if it's a date range format: "YYYY-MM-DD to YYYY-MM-DD"
        if " to " in time_period:
            return TimePeriodParser._parse_date_range(time_period)

        # Check if it's a single date format: "YYYY-MM-DD"
        if re.match(r"^\d{4}-\d{2}-\d{2}$", time_period):
            return TimePeriodParser._parse_single_date(time_period)

        # Handle relative time periods
        now = datetime.now()

        if time_period == "last-week":
            days = 7
        elif time_period == "last-2-weeks":
            days = 14
        elif time_period == "last-month":
            days = 30
        elif time_period.endswith("-days"):
            # Extract number from "N-days" format
            match = re.match(r"(\d+)-days", time_period)
            if match:
                days = int(match.group(1))
            else:
                raise ValueError(f"Invalid time period format: {time_period}")
        else:
            raise ValueError(f"Unsupported time period: {time_period}")

        start_date = now - timedelta(days=days)
        end_date = now

        return start_date, end_date

    @staticmethod
    def _parse_date_range(time_period: str) -> Tuple[datetime, datetime]:
        """Parse date range format: 'YYYY-MM-DD to YYYY-MM-DD'."""
        try:
            start_str, end_str = time_period.split(" to ")
            start_date = datetime.strptime(start_str.strip(), "%Y-%m-%d")
            end_date = datetime.strptime(end_str.strip(), "%Y-%m-%d")

            # Set end_date to end of day (23:59:59)
            end_date = end_date.replace(hour=23, minute=59, second=59)

            if start_date > end_date:
                raise ValueError("Start date must be before or equal to end date")

            return start_date, end_date
        except ValueError as e:
            if "time data" in str(e):
                raise ValueError(
                    f"Invalid date format in range '{time_period}'. Use YYYY-MM-DD format."
                )
            raise

    @staticmethod
    def _parse_single_date(time_period: str) -> Tuple[datetime, datetime]:
        """Parse single date format: 'YYYY-MM-DD' (treats as single day range)."""
        try:
            single_date = datetime.strptime(time_period, "%Y-%m-%d")
            start_date = single_date
            end_date = single_date.replace(hour=23, minute=59, second=59)
            return start_date, end_date
        except ValueError:
            raise ValueError(f"Invalid date format '{time_period}'. Use YYYY-MM-DD format.")


class IssueAdherenceService:
    """Service class for issue adherence analysis."""

    def __init__(self):
        self.jira_assistant = JiraAssistant()
        self.logger = LogManager.get_instance().get_logger("IssueAdherenceService")
        self.time_parser = TimePeriodParser()

    def analyze_issue_adherence(
        self,
        project_key: str,
        time_period: str,
        issue_types: List[str],
        team: Optional[str] = None,
        status_categories: Optional[List[str]] = None,
        include_no_due_date: bool = False,
        verbose: bool = False,
        output_file: Optional[str] = None,
    ) -> Dict:
        """
        Analyze issue adherence for issues that were resolved within a specified time period.

        Args:
            project_key (str): JIRA project key
            time_period (str): Time period to analyze
            issue_types (List[str]): List of issue types to include
            team (Optional[str]): Team name to filter by
            status_categories (List[str]): Status categories to include
            include_no_due_date (bool): Include issues without due dates
            verbose (bool): Enable verbose output
            output_file (Optional[str]): Output file path

        Returns:
            Dict: Analysis results with metrics and issue details
        """
        try:
            self.logger.info(f"Starting issue adherence analysis for project {project_key}")

            # Parse time period
            start_date, end_date = self.time_parser.parse_time_period(time_period)

            # Build JQL query
            jql_query = self._build_jql_query(
                project_key, start_date, end_date, issue_types, team, status_categories
            )

            self.logger.info(f"Executing JQL query: {jql_query}")

            # Fetch issues - JIRA limits maxResults to 100 when requesting custom fields
            # Our fetch_issues method handles pagination automatically to get all results
            issues = self.jira_assistant.fetch_issues(
                jql_query=jql_query,
                fields=(
                    "key,summary,issuetype,status,duedate,"
                    "resolutiondate,assignee,customfield_10851,"
                    "customfield_10265"
                ),
                max_results=100,  # Use 100 to work with JIRA limitation on custom fields
                expand_changelog=False,
            )

            self.logger.info(f"Fetched {len(issues)} issues for analysis")

            # Analyze each issue
            adherence_results = []
            issues_without_due_date = 0
            for issue in issues:
                result = self._analyze_issue_adherence(issue)

                # Filter issues without due dates if not requested
                if not include_no_due_date and result.adherence_status == "no_due_date":
                    issues_without_due_date += 1
                    continue

                adherence_results.append(result)

            # Log filtering summary
            if issues_without_due_date > 0:
                self.logger.info(
                    f"Excluded {issues_without_due_date} issues without due dates. "
                    f"Use --include-no-due-date to include them."
                )

            self.logger.info(f"Analyzing {len(adherence_results)} issues with due dates")

            # Calculate metrics
            metrics = self._calculate_metrics(adherence_results)

            # Prepare results
            results = {
                "analysis_metadata": {
                    "project_key": project_key,
                    "time_period": time_period,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "issue_types": issue_types,
                    "team": team,
                    "status_categories": status_categories,
                    "include_no_due_date": include_no_due_date,
                    "analysis_date": datetime.now().isoformat(),
                },
                "metrics": metrics,
                "issues": [self._result_to_dict(result) for result in adherence_results],
            }

            # Save to file if specified
            if output_file:
                self._save_results(results, output_file)
            else:
                # Generate default output file in organized structure
                output_path = OutputManager.get_output_path(
                    "issue-adherence", f"adherence_{project_key}"
                )
                self._save_results(results, output_path)

            # Print verbose output if requested
            if verbose:
                self._print_verbose_results(adherence_results, metrics)

            return results

        except Exception as e:
            # Check if this is a JQL error and provide helpful suggestions
            error_str = str(e)
            if ("400" in error_str and ("does not exist for the field 'type'" in error_str or "invalid issue type" in error_str.lower())):
                self.logger.error("JQL Query Error (400): Invalid issue type detected.")
                self.logger.error(f"Original query: {jql_query}")
                
                # Try to fetch available issue types for the project
                try:
                    available_types = self.jira_assistant.fetch_project_issue_types(project_key)
                    type_names = [t.get('name') for t in available_types]
                    self.logger.error(f"Available issue types for project {project_key}: {type_names}")
                    
                    # Check which provided types are not available
                    invalid_types = [t for t in issue_types if t not in type_names]
                    if invalid_types:
                        self.logger.error(f"Invalid issue types provided: {invalid_types}")
                        self.logger.error("Suggested command with valid types:")
                        valid_types = [t for t in issue_types if t in type_names]
                        suggested_types = ','.join(valid_types) if valid_types else ','.join(type_names[:5])
                        self.logger.error(f"--issue-types '{suggested_types}'")
                except Exception as metadata_error:
                    self.logger.warning(f"Could not fetch project metadata: {metadata_error}")
                
            self.logger.error(f"Error in issue adherence analysis: {e}")
            raise

    def _build_jql_query(
        self,
        project_key: str,
        start_date: datetime,
        end_date: datetime,
        issue_types: List[str],
        team: Optional[str],
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
        jql_parts.append(f"resolved >= '{start_date_str}' AND resolved <= '{end_date_str}'")

        # Team filter
        if team:
            jql_parts.append(f"'Squad[Dropdown]' = '{team}'")

        # Status categories
        if status_categories:
            categories_str = "', '".join(status_categories)
            jql_parts.append(f"statusCategory in ('{categories_str}')")

        return " AND ".join(jql_parts)

    def _analyze_issue_adherence(self, issue: Dict) -> IssueAdherenceResult:
        """Analyze adherence for a single issue."""

        fields = issue.get("fields", {})

        # Extract issue information
        issue_key = issue.get("key", "")
        summary = fields.get("summary", "")
        issue_type = fields.get("issuetype", {}).get("name", "")
        status = fields.get("status", {}).get("name", "")
        status_category = fields.get("status", {}).get("statusCategory", {}).get("name", "")
        due_date = fields.get("duedate")
        resolution_date = fields.get("resolutiondate")
        assignee = fields.get("assignee", {}).get("displayName") if fields.get("assignee") else None
        team = (
            fields.get("customfield_10265", {}).get("value")
            if fields.get("customfield_10265")
            else None
        )

        # Determine adherence status
        adherence_status, days_difference = self._determine_adherence_status(
            due_date, resolution_date, status_category
        )

        return IssueAdherenceResult(
            issue_key=issue_key,
            summary=summary,
            issue_type=issue_type,
            status=status,
            status_category=status_category,
            due_date=due_date,
            resolution_date=resolution_date,
            assignee=assignee,
            team=team,
            adherence_status=adherence_status,
            days_difference=days_difference,
        )

    def _determine_adherence_status(
        self,
        due_date: Optional[str],
        resolution_date: Optional[str],
        status_category: str,
    ) -> Tuple[str, Optional[int]]:
        """Determine adherence status for an issue."""

        if not due_date:
            return "no_due_date", None

        due_date_obj = self._parse_datetime(due_date)

        if status_category == "Done" and resolution_date:
            # Issue is completed
            resolution_date_obj = self._parse_datetime(resolution_date)
            days_difference = (resolution_date_obj - due_date_obj).days

            if days_difference < 0:
                return "early", days_difference
            elif days_difference == 0:
                return "on_time", days_difference
            else:
                return "late", days_difference
        else:
            # Issue is not completed
            now = datetime.now()
            days_difference = (now - due_date_obj).days

            if days_difference > 0:
                return "overdue", days_difference
            else:
                return "in_progress", days_difference

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
                return datetime.fromisoformat(date_string.replace("Z", "+00:00")).replace(
                    tzinfo=None
                )
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

    def _calculate_metrics(self, results: List[IssueAdherenceResult]) -> Dict:
        """Calculate adherence metrics."""

        total_issues = len(results)

        if total_issues == 0:
            return {
                "total_issues": 0,
                "issues_with_due_dates": 0,
                "early": 0,
                "on_time": 0,
                "late": 0,
                "overdue": 0,
                "no_due_date": 0,
                "in_progress": 0,
                "adherence_rate": 0.0,
                "early_percentage": 0.0,
                "on_time_percentage": 0.0,
                "late_percentage": 0.0,
                "overdue_percentage": 0.0,
                "no_due_date_percentage": 0.0,
                "in_progress_percentage": 0.0,
            }

        # Count by status
        status_counts: Dict[str, int] = {}
        for result in results:
            status = result.adherence_status
            status_counts[status] = status_counts.get(status, 0) + 1

        early = status_counts.get("early", 0)
        on_time = status_counts.get("on_time", 0)
        late = status_counts.get("late", 0)
        overdue = status_counts.get("overdue", 0)
        no_due_date = status_counts.get("no_due_date", 0)
        in_progress = status_counts.get("in_progress", 0)

        issues_with_due_dates = total_issues - no_due_date
        completed_issues = early + on_time + late

        # Calculate percentages
        early_percentage = (early / total_issues * 100) if total_issues > 0 else 0
        on_time_percentage = (on_time / total_issues * 100) if total_issues > 0 else 0
        late_percentage = (late / total_issues * 100) if total_issues > 0 else 0
        overdue_percentage = (overdue / total_issues * 100) if total_issues > 0 else 0
        no_due_date_percentage = (no_due_date / total_issues * 100) if total_issues > 0 else 0
        in_progress_percentage = (in_progress / total_issues * 100) if total_issues > 0 else 0

        # Calculate adherence rate (early + on-time completion out of completed issues)
        adherence_rate = ((early + on_time) / completed_issues * 100) if completed_issues > 0 else 0

        return {
            "total_issues": total_issues,
            "issues_with_due_dates": issues_with_due_dates,
            "early": early,
            "on_time": on_time,
            "late": late,
            "overdue": overdue,
            "no_due_date": no_due_date,
            "in_progress": in_progress,
            "adherence_rate": adherence_rate,
            "early_percentage": early_percentage,
            "on_time_percentage": on_time_percentage,
            "late_percentage": late_percentage,
            "overdue_percentage": overdue_percentage,
            "no_due_date_percentage": no_due_date_percentage,
            "in_progress_percentage": in_progress_percentage,
        }

    def _result_to_dict(self, result: IssueAdherenceResult) -> Dict:
        """Convert result to dictionary."""
        return {
            "issue_key": result.issue_key,
            "summary": result.summary,
            "issue_type": result.issue_type,
            "status": result.status,
            "status_category": result.status_category,
            "due_date": result.due_date,
            "resolution_date": result.resolution_date,
            "assignee": result.assignee,
            "team": result.team,
            "adherence_status": result.adherence_status,
            "days_difference": result.days_difference,
        }

    def _save_results(self, results: Dict, output_file: str):
        """Save results to JSON file."""
        try:
            JSONManager.write_json(results, output_file)
            self.logger.info(f"Results saved to {output_file}")
        except Exception as e:
            self.logger.error(f"Failed to save results to {output_file}: {e}")
            raise

    def _print_verbose_results(self, results: List[IssueAdherenceResult], _metrics: Dict):
        """Print verbose results to console."""
        print("\n" + "=" * 80)
        print("DETAILED ISSUE ADHERENCE ANALYSIS")
        print("=" * 80)

        # Group by adherence status
        status_groups: Dict[str, List[IssueAdherenceResult]] = {}
        for result in results:
            status = result.adherence_status
            if status not in status_groups:
                status_groups[status] = []
            status_groups[status].append(result)

        for status, issues in status_groups.items():
            print(f"\n{status.upper().replace('_', ' ')} ISSUES ({len(issues)}):")
            print("-" * 40)

            for issue in issues:
                print(f"  {issue.issue_key}: {issue.summary}")
                print(f"    Type: {issue.issue_type} | Status: {issue.status}")
                print(f"    Due Date: {issue.due_date or 'Not set'}")
                print(f"    Resolution Date: {issue.resolution_date or 'Not resolved'}")
                print(f"    Assignee: {issue.assignee or 'Unassigned'}")
                print(f"    Team: {issue.team or 'Not set'}")
                if issue.days_difference is not None:
                    if issue.adherence_status == "early":
                        print(f"    Days Early: {abs(issue.days_difference)}")
                    elif issue.adherence_status == "late":
                        print(f"    Days Late: {issue.days_difference}")
                    elif issue.adherence_status == "on_time":
                        print("    Completed on Due Date")
                    elif issue.adherence_status == "overdue":
                        print(f"    Days Overdue: {issue.days_difference}")
                print()

        print("=" * 80)
