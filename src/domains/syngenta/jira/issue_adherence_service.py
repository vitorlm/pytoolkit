"""
JIRA Issue Adherence Service

This service provides functionality to analyze issue adherence by checking completion
status against due dates for issues that were resolved within specified time periods.
"""

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import statistics
from collections import defaultdict

from utils.data.json_manager import JSONManager
from utils.file_manager import FileManager
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
            raise ValueError(
                f"Invalid date format '{time_period}'. Use YYYY-MM-DD format."
            )


class IssueAdherenceService:
    """Service class for issue adherence analysis."""

    def __init__(self):
        self.jira_assistant = JiraAssistant()
        self.logger = LogManager.get_instance().get_logger("IssueAdherenceService")
        self.time_parser = TimePeriodParser()

    def _get_adherence_status_emoji(self, status: str) -> str:
        """Get emoji for adherence status."""
        emoji_map = {
            "early": "🟣",
            "on_time": "✅",
            "late": "🟡",
            "overdue": "🔴",
            "no_due_date": "⚪",
            "in_progress": "🔵",
        }
        return emoji_map.get(status, "❓")

    def _get_risk_level_emoji(self, adherence_rate: float) -> str:
        """Get emoji for risk level based on adherence rate."""
        if adherence_rate >= 80:
            return "🟢"  # Low risk
        elif adherence_rate >= 60:
            return "🟡"  # Medium risk
        else:
            return "🔴"  # High risk

    def _get_risk_assessment(self, adherence_rate: float) -> str:
        """Get risk assessment text based on adherence rate."""
        if adherence_rate >= 80:
            return "Low Risk - Excellent adherence to due dates"
        elif adherence_rate >= 60:
            return "Medium Risk - Moderate adherence issues"
        else:
            return "High Risk - Significant adherence concerns"

    def analyze_issue_adherence(
        self,
        project_key: str,
        time_period: str,
        issue_types: List[str],
        teams: Optional[List[str]] = None,
        status: Optional[List[str]] = None,
        include_no_due_date: bool = False,
        verbose: bool = False,
        output_file: Optional[str] = None,
        output_format: str = "console",
        weighted_adherence: bool = False,
        enable_extended: bool = False,
        early_tolerance_days: int = 1,
        early_weight: float = 1.0,
        late_weight: float = 3.0,
        no_due_penalty: float = 15.0,
    ) -> Dict:
        """
        Analyze issue adherence for issues that were resolved within a specified time period.

        Args:
            project_key (str): JIRA project key
            time_period (str): Time period to analyze
            issue_types (List[str]): List of issue types to include
            teams (Optional[List[str]]): List of team names to filter by
            status (List[str]): Status to include
            include_no_due_date (bool): Include issues without due dates
            verbose (bool): Enable verbose output
            output_file (Optional[str]): Output file path
            output_format (str): Output format: "console", "json", or "md"

        Returns:
            Dict: Analysis results with metrics and issue details
        """
        try:
            self.logger.info(
                f"Starting issue adherence analysis for project {project_key}"
            )

            # Parse time period
            start_date, end_date = self.time_parser.parse_time_period(time_period)

            # Build JQL query
            jql_query = self._build_jql_query(
                project_key, start_date, end_date, issue_types, teams, status
            )

            self.logger.info(f"Executing JQL query: {jql_query}")

            # Fetch issues - JIRA limits maxResults to 100 when requesting custom fields
            # Our fetch_issues method handles pagination automatically to get all results
            issues = self.jira_assistant.fetch_issues(
                jql_query=jql_query,
                fields=(
                    "key,summary,issuetype,status,duedate,resolutiondate,assignee,customfield_10851,customfield_10265"
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

            # Logging context depending on inclusion of no-due-date issues
            if include_no_due_date:
                self.logger.info(
                    f"Analyzing {len(adherence_results)} issues (including 'no due date' when applicable)"
                )
            else:
                self.logger.info(
                    f"Analyzing {len(adherence_results)} issues with due dates"
                )

            # Calculate metrics (legacy adherence; DO NOT CHANGE)
            metrics = self._calculate_metrics(adherence_results)

            # Calculate statistical insights (only in extended mode)
            statistical_insights = (
                self._calculate_statistical_insights(adherence_results)
                if enable_extended
                else None
            )

            # Calculate segmentation analysis
            segmentation_analysis = self._calculate_segmentation_analysis(
                adherence_results
            )

            # Calculate due date coverage analysis
            due_date_coverage = self._calculate_due_date_coverage_analysis(
                issues, adherence_results
            )

            # Prepare results with schema version for backward compatibility
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

            results = {
                "schema_version": "2.0",
                "analysis_metadata": {
                    "project_key": project_key,
                    "time_period": time_period,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "issue_types": issue_types,
                    "team": team_label,
                    "teams": teams,
                    "status": status,
                    "include_no_due_date": include_no_due_date,
                    "analysis_date": datetime.now().isoformat(),
                },
                "metrics": metrics,
                **(
                    {"statistical_insights": statistical_insights}
                    if statistical_insights
                    else {}
                ),
                "segmentation_analysis": segmentation_analysis,
                "due_date_coverage": due_date_coverage,
                "issues": [
                    self._result_to_dict(result) for result in adherence_results
                ],
            }

            # Delivery time distribution (based on absolute deviation from due date for completed items)
            time_dist = self._calculate_delivery_time_distribution(adherence_results)
            results["time_distribution"] = time_dist

            # Optional: calculate weighted adherence without altering the legacy metric
            if weighted_adherence:
                weighted_metrics = self._calculate_weighted_metrics(
                    adherence_results=adherence_results,
                    include_no_due_date=include_no_due_date,
                    early_tolerance_days=early_tolerance_days,
                    early_weight=early_weight,
                    late_weight=late_weight,
                    no_due_penalty=no_due_penalty,
                )
                results["weighted_metrics"] = weighted_metrics

            # Provide a dedicated list of issues without due date for convenience in consumers
            if include_no_due_date:
                no_due_date_issues = [
                    {
                        "issue_key": r.issue_key,
                        "summary": r.summary,
                        "team": r.team,
                        "assignee": r.assignee,
                    }
                    for r in adherence_results
                    if r.adherence_status == "no_due_date"
                ]
                results["issues_without_due_date"] = no_due_date_issues

            # Save to file if specified or generate default output
            output_path = None
            if output_file:
                # When output file is specified, use the format explicitly or infer from extension
                if output_format in ["json", "md"]:
                    # Use the specified format
                    if output_format == "md":
                        # Remove extension if present and let the method add .md
                        base_path = output_file.replace(".json", "").replace(".md", "")
                        output_path = self._save_results_with_format(
                            results, base_path, "md"
                        )
                    else:
                        # For JSON, keep original logic
                        output_path = self._save_results_with_format(
                            results, output_file, "json"
                        )
                else:
                    # Infer format from file extension or default to JSON
                    if output_file.endswith(".md"):
                        base_path = output_file.replace(".md", "")
                        output_path = self._save_results_with_format(
                            results, base_path, "md"
                        )
                    else:
                        output_path = self._save_results_with_format(
                            results, output_file, "json"
                        )
            elif output_format in ["json", "md"]:
                # Generate default output file in organized structure (per-day folder)
                from datetime import datetime as _dt

                sub_dir = f"issue-adherence_{_dt.now().strftime('%Y%m%d')}"
                base_path = OutputManager.get_output_path(
                    sub_dir, f"adherence_{project_key}"
                )
                output_path = self._save_results_with_format(
                    results, base_path, output_format
                )

            # Add output file path to results for command feedback
            if output_path:
                results["output_file"] = output_path

            # Print verbose output if requested
            if verbose:
                self._print_verbose_results(adherence_results, metrics)

            return results

        except Exception as e:
            # Check if this is a JQL error and provide helpful suggestions
            error_str = str(e)
            if "400" in error_str and (
                "does not exist for the field 'type'" in error_str
                or "invalid issue type" in error_str.lower()
            ):
                self.logger.error("JQL Query Error (400): Invalid issue type detected.")
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

            self.logger.error(f"Error in issue adherence analysis: {e}")
            raise

    def _build_jql_query(
        self,
        project_key: str,
        start_date: datetime,
        end_date: datetime,
        issue_types: List[str],
        teams: Optional[List[str]],
        status: Optional[List[str]],
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

        # Team filter (supports multiple teams)
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

        # Status
        if status:
            status_str = "', '".join(status)
            jql_parts.append(f"status in ('{status_str}')")

        return " AND ".join(jql_parts)

    def _analyze_issue_adherence(self, issue: Dict) -> IssueAdherenceResult:
        """Analyze adherence for a single issue."""

        fields = issue.get("fields", {})

        # Extract issue information
        issue_key = issue.get("key", "")
        summary = fields.get("summary", "")
        issue_type = fields.get("issuetype", {}).get("name", "")
        status = fields.get("status", {}).get("name", "")
        status_category = (
            fields.get("status", {}).get("statusCategory", {}).get("name", "")
        )
        due_date = fields.get("duedate")
        resolution_date = fields.get("resolutiondate")
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
                "no_due_date": 0,
                "in_progress": 0,
                "adherence_rate": 0.0,
                "early_percentage": 0.0,
                "on_time_percentage": 0.0,
                "late_percentage": 0.0,
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
        # 'overdue' is not part of this analysis output
        no_due_date = status_counts.get("no_due_date", 0)
        in_progress = status_counts.get("in_progress", 0)

        issues_with_due_dates = total_issues - no_due_date
        completed_issues = early + on_time + late

        # Calculate percentages
        early_percentage = (early / total_issues * 100) if total_issues > 0 else 0
        on_time_percentage = (on_time / total_issues * 100) if total_issues > 0 else 0
        late_percentage = (late / total_issues * 100) if total_issues > 0 else 0
        no_due_date_percentage = (
            (no_due_date / total_issues * 100) if total_issues > 0 else 0
        )
        in_progress_percentage = (
            (in_progress / total_issues * 100) if total_issues > 0 else 0
        )

        # Calculate adherence rate (early + on-time completion out of completed issues)
        adherence_rate = (
            ((early + on_time) / completed_issues * 100) if completed_issues > 0 else 0
        )

        return {
            "total_issues": total_issues,
            "issues_with_due_dates": issues_with_due_dates,
            "early": early,
            "on_time": on_time,
            "late": late,
            "no_due_date": no_due_date,
            "in_progress": in_progress,
            "adherence_rate": adherence_rate,
            "early_percentage": early_percentage,
            "on_time_percentage": on_time_percentage,
            "late_percentage": late_percentage,
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

    def _calculate_weighted_metrics(
        self,
        adherence_results: List[IssueAdherenceResult],
        include_no_due_date: bool,
        early_tolerance_days: int,
        early_weight: float,
        late_weight: float,
        no_due_penalty: float,
    ) -> Dict:
        """
        Calculate weighted adherence based on asymmetric linear penalties with enhanced scoring.

        Rules:
          - Early tolerance applies only to earliness; lateness has zero tolerance.
          - Early is penalized less (early_weight) than late (late_weight).
          - Completed issues without due date (when included) receive a fixed penalty.
          - Progressive penalties: severe lateness gets higher weight multipliers.

        Returns a dict with weighted_adherence and parameters.
        """
        scores: List[float] = []
        individual_scores: List[Dict] = []
        counted_by_category: Dict[str, int] = {
            "early": 0,
            "on_time": 0,
            "late": 0,
            "no_due_date": 0,
        }
        early_penalty_total = 0.0
        late_penalty_total = 0.0
        ndd_penalty_total = 0.0
        total_penalty_raw = 0.0
        total_penalty_capped = 0.0
        cap_per_item = 100.0
        early_penalty_capped_total = 0.0
        late_penalty_capped_total = 0.0
        ndd_penalty_capped_total = 0.0

        for r in adherence_results:
            status = r.adherence_status

            # Completed with due date
            if status in ("early", "on_time", "late") and r.days_difference is not None:
                d = r.days_difference  # negative = early; positive = late; 0 = on_time

                # Enhanced penalties with progressive scaling for severe lateness
                earliness_beyond = max(0, -d - max(0, early_tolerance_days))
                lateness_beyond = max(0, d)  # no tolerance for lateness

                # Progressive penalty for severe lateness (exponential scaling after 7 days)
                if lateness_beyond > 7:
                    severe_multiplier = (
                        1 + (lateness_beyond - 7) * 0.2
                    )  # 20% additional penalty per day after 7
                    late_penalty_multiplier = min(3.0, severe_multiplier)  # Cap at 3x
                else:
                    late_penalty_multiplier = 1.0

                early_p = early_weight * earliness_beyond
                late_p = late_weight * lateness_beyond * late_penalty_multiplier
                penalty_raw = early_p + late_p
                penalty_capped = min(cap_per_item, penalty_raw)
                score = 100.0 - penalty_capped
                scores.append(score)

                # Track individual scores for detailed analysis
                individual_scores.append(
                    {
                        "issue_key": r.issue_key,
                        "days_difference": d,
                        "penalty_raw": round(penalty_raw, 2),
                        "penalty_capped": round(penalty_capped, 2),
                        "score": round(score, 2),
                        "status": status,
                        "team": r.team,
                        "assignee": r.assignee,
                    }
                )

                counted_by_category[status] += 1
                early_penalty_total += early_p
                late_penalty_total += late_p
                total_penalty_raw += penalty_raw
                total_penalty_capped += penalty_capped
                # Distribute capped total proportionally between early/late components
                if penalty_raw > 0:
                    factor = penalty_capped / penalty_raw
                    early_penalty_capped_total += early_p * factor
                    late_penalty_capped_total += late_p * factor
                else:
                    # No penalty components
                    pass

            # Completed without due date (only if included)
            elif status == "no_due_date" and include_no_due_date:
                # Consider only completed items; by construction, 'no_due_date' may be done or not
                # We rely on status_category/resolution_date to ensure completion
                if r.status_category == "Done" or r.resolution_date:
                    penalty_raw = max(0.0, float(no_due_penalty))
                    penalty_capped = min(cap_per_item, penalty_raw)
                    score = 100.0 - penalty_capped
                    scores.append(score)

                    individual_scores.append(
                        {
                            "issue_key": r.issue_key,
                            "days_difference": None,
                            "penalty_raw": round(penalty_raw, 2),
                            "penalty_capped": round(penalty_capped, 2),
                            "score": round(score, 2),
                            "status": status,
                            "team": r.team,
                            "assignee": r.assignee,
                        }
                    )

                    counted_by_category[status] += 1
                    ndd_penalty_total += penalty_raw
                    total_penalty_raw += penalty_raw
                    total_penalty_capped += penalty_capped
                    ndd_penalty_capped_total += penalty_capped

        weighted_adherence = (sum(scores) / len(scores)) if scores else 0.0
        total_items = len(scores)
        avg_penalty_capped = (
            (total_penalty_capped / total_items) if total_items > 0 else 0.0
        )
        avg_penalty_raw = (total_penalty_raw / total_items) if total_items > 0 else 0.0

        # Calculate score distribution statistics
        score_stats = {}
        if scores:
            score_stats = {
                "mean": round(statistics.mean(scores), 2),
                "median": round(statistics.median(scores), 2),
                "std_dev": round(statistics.stdev(scores), 2)
                if len(scores) > 1
                else 0.0,
                "min": round(min(scores), 2),
                "max": round(max(scores), 2),
            }

        return {
            "weighted_adherence": round(weighted_adherence, 2),
            "parameters": {
                "early_tolerance_days": early_tolerance_days,
                "early_weight": early_weight,
                "late_weight": late_weight,
                "no_due_penalty": no_due_penalty,
                "progressive_scaling_enabled": True,
                "severe_lateness_threshold": 7,
            },
            "included_items": total_items,
            "included_breakdown": counted_by_category,
            "score_statistics": score_stats,
            "individual_scores": individual_scores,
            "penalties": {
                "total_capped": round(total_penalty_capped, 2),
                "total_raw": round(total_penalty_raw, 2),
                "early_total": round(early_penalty_total, 2),
                "late_total": round(late_penalty_total, 2),
                "no_due_total": round(ndd_penalty_total, 2),
                "avg_per_item_capped": round(avg_penalty_capped, 2),
                "avg_per_item_raw": round(avg_penalty_raw, 2),
                "cap_per_item": cap_per_item,
                "late_total_capped": round(late_penalty_capped_total, 2),
                "early_total_capped": round(early_penalty_capped_total, 2),
                "no_due_total_capped": round(ndd_penalty_capped_total, 2),
            },
        }

    def _save_results_with_format(
        self, results: Dict, base_path: str, output_format: str
    ) -> str:
        """
        Save results in the specified format.

        Args:
            results (Dict): Analysis results
            base_path (str): Base file path (without extension)
            output_format (str): Format to save in ("json" or "md")

        Returns:
            str: Path to saved file
        """
        try:
            if output_format == "md":
                # Generate markdown content
                markdown_content = self._format_as_markdown(results)

                # Use OutputManager to save markdown (per-day folder)
                from datetime import datetime as _dt

                sub_dir = f"issue-adherence_{_dt.now().strftime('%Y%m%d')}"
                output_path = OutputManager.save_markdown_report(
                    content=markdown_content,
                    sub_dir=sub_dir,
                    file_basename=f"adherence_{results.get('analysis_metadata', {}).get('project_key', 'unknown')}",
                )

                self.logger.info(f"Markdown report saved to {output_path}")
                return output_path

            else:  # Default to JSON
                # Use OutputManager to save JSON (per-day folder)
                from datetime import datetime as _dt

                sub_dir = f"issue-adherence_{_dt.now().strftime('%Y%m%d')}"
                output_path = OutputManager.save_json_report(
                    data=results,
                    sub_dir=sub_dir,
                    file_basename=f"adherence_{results.get('analysis_metadata', {}).get('project_key', 'unknown')}",
                )

                self.logger.info(f"JSON report saved to {output_path}")
                return output_path

        except Exception as e:
            self.logger.error(f"Failed to save results in {output_format} format: {e}")
            raise

    def _save_results(self, results: Dict, output_file: str):
        """Save results to JSON file."""
        try:
            # Ensure the directory exists before writing the file
            # Extract directory path from file path
            output_dir = self._extract_directory_path(output_file)
            if output_dir and not FileManager.is_folder(output_dir):
                FileManager.create_folder(output_dir)
                self.logger.info(f"Created output directory: {output_dir}")

            JSONManager.write_json(results, output_file)
            self.logger.info(f"Results saved to {output_file}")
        except Exception as e:
            self.logger.error(f"Failed to save results to {output_file}: {e}")
            raise

    def _extract_directory_path(self, file_path: str) -> Optional[str]:
        """
        Extract directory path from file path in a cross-platform way.

        Args:
            file_path (str): Full file path

        Returns:
            Optional[str]: Directory path or None if no directory component
        """
        # Find the last path separator (works for both / and \)
        last_sep_pos = max(file_path.rfind("/"), file_path.rfind("\\"))

        if (
            last_sep_pos > 0
        ):  # > 0 to ensure we don't return empty string for paths like "file.json"
            return file_path[:last_sep_pos]

        return None

    def _print_verbose_results(
        self, results: List[IssueAdherenceResult], _metrics: Dict
    ):
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
                print()

        print("=" * 80)

    def _format_as_markdown(self, results: Dict) -> str:
        """
        Format adherence analysis results as markdown optimized for AI consumption.

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
        analysis_date = metadata.get("analysis_date", "")

        # Calculate risk assessment
        adherence_rate = metrics.get("adherence_rate", 0)
        risk_emoji = self._get_risk_level_emoji(adherence_rate)
        risk_assessment = self._get_risk_assessment(adherence_rate)

        md_content = []

        # Header
        md_content.append(f"# {risk_emoji} JIRA Issue Adherence Analysis Report")
        md_content.append("")
        md_content.append(f"**Project:** {project_key}")
        md_content.append(f"**Analysis Period:** {time_period}")
        if start_date and end_date:
            md_content.append(f"**Date Range:** {start_date[:10]} to {end_date[:10]}")
        md_content.append(f"**Issue Types:** {', '.join(issue_types)}")
        if team:
            md_content.append(f"**Team Filter:** {team}")
        md_content.append(
            f"**Generated:** {analysis_date[:19] if analysis_date else 'Unknown'}"
        )
        md_content.append("")

        # Executive Summary
        md_content.append("## 📊 Executive Summary")
        md_content.append("")
        md_content.append(f"**Adherence:** {adherence_rate:.1f}%")
        md_content.append(f"**Risk Assessment:** {risk_assessment}")
        weighted = results.get("weighted_metrics")
        if weighted:
            md_content.append(
                f"**Weighted Adherence:** {weighted.get('weighted_adherence', 0):.1f}%"
            )
        md_content.append("")
        md_content.append(
            f"- **Total Issues Analyzed:** {metrics.get('total_issues', 0)}"
        )
        md_content.append(
            f"- **Issues with Due Dates:** {metrics.get('issues_with_due_dates', 0)}"
        )
        md_content.append(
            f"- **Completed Issues:** {metrics.get('early', 0) + metrics.get('on_time', 0) + metrics.get('late', 0)}"
        )
        md_content.append("")

        # Adherence Metrics (details)
        if weighted:
            params = weighted.get("parameters", {})
            md_content.append("## 📐 Adherence Metrics")
            md_content.append("")
            md_content.append(
                "- Legacy Adherence: Computed as (early + on-time) / completed"
            )
            md_content.append(
                "- Weighted Adherence: Averages per-issue scores with asymmetric penalties (lateness weighted more, early has small tolerance; completed issues without due date get a fixed penalty)"
            )
            md_content.append("- Parameters used:")
            md_content.append(
                f"  - Early tolerance (days): {params.get('early_tolerance_days')}"
            )
            md_content.append(
                f"  - Early weight (pt/day): {params.get('early_weight')}"
            )
            md_content.append(f"  - Late weight (pt/day): {params.get('late_weight')}")
            md_content.append(
                f"  - No-due penalty (pts): {params.get('no_due_penalty')}"
            )
            md_content.append("")

        # Statistical Insights
        statistical_insights = results.get("statistical_insights")
        if (
            statistical_insights
            and statistical_insights.get("total_completed_with_due_dates", 0) > 0
        ):
            md_content.append("## 📊 Statistical Insights")
            md_content.append("")
            md_content.append(
                "> Notes\n> - Percentiles: e.g., P90 means 90% of items are at or below that value.\n> - Outliers (IQR): values beyond 1.5×IQR from the 25th–75th percentiles."
            )
            md_content.append("")

            delivery_stats = statistical_insights.get("delivery_time_stats", {})
            percentiles = statistical_insights.get("percentile_analysis", {})
            outliers = statistical_insights.get("outlier_analysis", {})

            md_content.append("### Delivery Time Statistics")
            md_content.append("")
            md_content.append("| Metric | Value |")
            md_content.append("|--------|-------|")
            md_content.append(f"| Mean | {delivery_stats.get('mean', 0):.1f} days |")
            md_content.append(
                f"| Median | {delivery_stats.get('median', 0):.1f} days |"
            )
            md_content.append(
                f"| Standard Deviation | {delivery_stats.get('std_dev', 0):.1f} days |"
            )
            md_content.append(
                f"| Range | {delivery_stats.get('min', 0)} to {delivery_stats.get('max', 0)} days |"
            )
            md_content.append("")

            md_content.append("### Percentile Analysis")
            md_content.append("")
            md_content.append("| Percentile | Days from Due Date |")
            md_content.append("|------------|-------------------|")
            for p in [10, 25, 50, 75, 90, 95]:
                value = percentiles.get(f"p{p}", 0)
                md_content.append(f"| P{p} | {value:.1f} |")
            md_content.append("")

            if outliers.get("outlier_count", 0) > 0:
                md_content.append("### Outlier Analysis")
                md_content.append("")
                md_content.append(
                    f"**{outliers.get('outlier_count', 0)} outliers detected** ({outliers.get('outlier_percentage', 0):.1f}% of completed issues)"
                )
                md_content.append("")
                extreme_late = outliers.get("extreme_late", [])
                extreme_early = outliers.get("extreme_early", [])
                if extreme_late:
                    md_content.append(
                        f"- **Extremely Late:** {len(extreme_late)} issues (worst: {max(extreme_late)} days)"
                    )
                if extreme_early:
                    md_content.append(
                        f"- **Extremely Early:** {len(extreme_early)} issues (earliest: {min(extreme_early)} days)"
                    )
                md_content.append("")

        # Segmentation Analysis
        segmentation = results.get("segmentation_analysis")
        if segmentation:
            md_content.append("## 👥 Segmentation Analysis")
            md_content.append("")

            # Team analysis
            by_team = segmentation.get("by_team", {})
            if len(by_team) > 1:
                md_content.append("### By Team")
                md_content.append("")
                md_content.append(
                    "| Team | Adherence Rate | Completed Issues | Early | On-Time | Late |"
                )
                md_content.append(
                    "|------|----------------|------------------|-------|---------|------|"
                )
                for team, team_metrics in by_team.items():
                    adherence = team_metrics.get("adherence_rate", 0)
                    counts = team_metrics.get("counts", {})
                    total = team_metrics.get("total_completed", 0)
                    risk_emoji = (
                        "🔴" if adherence < 60 else "🟡" if adherence < 80 else "🟢"
                    )
                    md_content.append(
                        f"| {team} | {adherence:.1f}% {risk_emoji} | {total} | {counts.get('early', 0)} | {counts.get('on_time', 0)} | {counts.get('late', 0)} |"
                    )
                md_content.append("")

            # Issue type analysis
            by_type = segmentation.get("by_issue_type", {})
            if len(by_type) > 1:
                md_content.append("### By Issue Type")
                md_content.append("")
                md_content.append(
                    "| Issue Type | Adherence Rate | Completed Issues | Early | On-Time | Late |"
                )
                md_content.append(
                    "|------------|----------------|------------------|-------|---------|------|"
                )
                for issue_type, type_metrics in by_type.items():
                    adherence = type_metrics.get("adherence_rate", 0)
                    counts = type_metrics.get("counts", {})
                    total = type_metrics.get("total_completed", 0)
                    risk_emoji = (
                        "🔴" if adherence < 60 else "🟡" if adherence < 80 else "🟢"
                    )
                    md_content.append(
                        f"| {issue_type} | {adherence:.1f}% {risk_emoji} | {total} | {counts.get('early', 0)} | {counts.get('on_time', 0)} | {counts.get('late', 0)} |"
                    )
                md_content.append("")

        # Due Date Coverage Analysis
        due_date_coverage = results.get("due_date_coverage")
        if due_date_coverage:
            md_content.append("## 📅 Due Date Coverage Analysis")
            md_content.append("")

            overall = due_date_coverage.get("overall", {})
            coverage_pct = overall.get("coverage_percentage", 0)
            md_content.append(f"**Overall Coverage:** {coverage_pct:.1f}%")
            md_content.append("")
            md_content.append(
                "> What is coverage?\n> - Share of issues that have a due date set. Higher coverage improves schedule reliability metrics."
            )
            md_content.append("")
            md_content.append("| Metric | Count |")
            md_content.append("|--------|-------|")
            md_content.append(
                f"| Issues with Due Dates | {overall.get('issues_with_due_dates', 0)} |"
            )
            md_content.append(
                f"| Issues without Due Dates | {overall.get('issues_without_due_dates', 0)} |"
            )
            md_content.append(f"| Total Issues | {overall.get('total_issues', 0)} |")
            md_content.append("")

            if coverage_pct < 80:
                md_content.append(
                    "⚠️ **Coverage below 80% - consider enforcing due date requirements**"
                )
                md_content.append("")

                # Show teams with worst coverage
                by_team_coverage = due_date_coverage.get("by_team", {})
                worst_teams = [
                    (team, metrics)
                    for team, metrics in by_team_coverage.items()
                    if metrics.get("coverage_rate", 0) < 80
                ]
                if worst_teams:
                    md_content.append("### Teams Needing Due Date Coverage Improvement")
                    md_content.append("")
                    md_content.append(
                        "| Team | Coverage Rate | With Due Date | Without Due Date |"
                    )
                    md_content.append(
                        "|------|---------------|---------------|------------------|"
                    )
                    for team, team_coverage_metrics in worst_teams[
                        :5
                    ]:  # Show top 5 worst
                        rate = team_coverage_metrics.get("coverage_rate", 0)
                        with_due = team_coverage_metrics.get("with_due_date", 0)
                        without_due = team_coverage_metrics.get("without_due_date", 0)
                        md_content.append(
                            f"| {team} | {rate:.1f}% | {with_due} | {without_due} |"
                        )
                    md_content.append("")

        # Adherence Metrics Breakdown
        md_content.append("## 📈 Adherence Metrics Breakdown")
        md_content.append("")
        md_content.append("| Status | Count | Percentage |")
        md_content.append("|--------|-------|------------|")

        status_metrics = [
            ("early", "🟡 Early Completion"),
            ("on_time", "🟢 On-time Completion"),
            ("late", "🔴 Late Completion"),
            ("no_due_date", "⚪ No Due Date"),
            ("in_progress", "🔵 In Progress"),
        ]

        for status_key, status_label in status_metrics:
            # Check if metrics has direct fields or is nested in 'counts'
            if "counts" in metrics:
                count = metrics["counts"].get(status_key, 0)
                percentage = metrics.get(f"{status_key}_percentage", 0)
            else:
                count = metrics.get(status_key, 0)
                percentage = metrics.get(f"{status_key}_percentage", 0)

            # Only show rows with data
            if count > 0 or status_key in [
                "early",
                "on_time",
                "late",
            ]:  # Always show main categories
                md_content.append(f"| {status_label} | {count} | {percentage:.1f}% |")

        md_content.append("")

        # Performance Analysis (prefer weighted if available)
        md_content.append("## 🎯 Performance Analysis")
        md_content.append("")

        early_count = metrics.get("early", 0)
        on_time_count = metrics.get("on_time", 0)
        late_count = metrics.get("late", 0)
        total_completed = early_count + on_time_count + late_count

        weighted_perf = results.get("weighted_metrics")
        if weighted_perf:
            wa = weighted_perf.get("weighted_adherence", 0.0)
            penalties = weighted_perf.get("penalties") or {}
            score_stats = weighted_perf.get("score_statistics", {})

            md_content.append(f"**✅ Weighted Completion Score:** {wa:.1f}%")
            md_content.append("")

            # Score statistics table
            if score_stats:
                md_content.append("### Weighted Score Statistics")
                md_content.append("")
                md_content.append("| Metric | Value |")
                md_content.append("|--------|-------|")
                md_content.append(f"| Mean Score | {score_stats.get('mean', 0):.1f}% |")
                md_content.append(
                    f"| Median Score | {score_stats.get('median', 0):.1f}% |"
                )
                md_content.append(
                    f"| Score Std Dev | {score_stats.get('std_dev', 0):.1f} |"
                )
                md_content.append(f"| Min Score | {score_stats.get('min', 0):.1f}% |")
                md_content.append(f"| Max Score | {score_stats.get('max', 0):.1f}% |")
                md_content.append("")

            # Penalty analysis table
            md_content.append("### Penalty Analysis")
            md_content.append("")
            md_content.append(
                "| Penalty Type | Total (Capped) | Total (Raw) | Avg per Item |"
            )
            md_content.append(
                "|--------------|----------------|-------------|--------------|"
            )
            md_content.append(
                f"| Late | {penalties.get('late_total_capped', 0.0):.1f} | {penalties.get('late_total', 0.0):.1f} | {(penalties.get('late_total_capped', 0.0) / max(1, weighted_perf.get('included_items', 1))):.1f} |"
            )
            md_content.append(
                f"| Early | {penalties.get('early_total_capped', 0.0):.1f} | {penalties.get('early_total', 0.0):.1f} | {(penalties.get('early_total_capped', 0.0) / max(1, weighted_perf.get('included_items', 1))):.1f} |"
            )
            md_content.append(
                f"| No Due Date | {penalties.get('no_due_total_capped', 0.0):.1f} | {penalties.get('no_due_total', 0.0):.1f} | {(penalties.get('no_due_total_capped', 0.0) / max(1, weighted_perf.get('included_items', 1))):.1f} |"
            )
            md_content.append(
                f"| **Total** | **{penalties.get('total_capped', 0.0):.1f}** | **{penalties.get('total_raw', 0.0):.1f}** | **{penalties.get('avg_per_item_capped', 0.0):.1f}** |"
            )
            md_content.append("")

            # Enhanced parameters info
            params = weighted_perf.get("parameters") or {}
            md_content.append("### Weighted Scoring Parameters")
            md_content.append("")
            md_content.append("| Parameter | Value | Description |")
            md_content.append("|-----------|-------|-------------|")
            md_content.append(
                f"| Early Tolerance | {params.get('early_tolerance_days', 0)} days | Grace period for early completion |"
            )
            md_content.append(
                f"| Early Weight | {params.get('early_weight', 0):.1f} pts/day | Penalty for early beyond tolerance |"
            )
            md_content.append(
                f"| Late Weight | {params.get('late_weight', 0):.1f} pts/day | Penalty for late completion |"
            )
            md_content.append(
                f"| No Due Penalty | {params.get('no_due_penalty', 0):.1f} pts | Fixed penalty for missing due date |"
            )
            if params.get("progressive_scaling_enabled"):
                md_content.append(
                    f"| Severe Late Threshold | {params.get('severe_lateness_threshold', 7)} days | Progressive penalty starts after this |"
                )
            md_content.append("")

            # Estimated deviation days and affected items for leadership view
            lw = float(params.get("late_weight", 0) or 0)
            ew = float(params.get("early_weight", 0) or 0)
            ndp = float(params.get("no_due_penalty", 0) or 0)
            late_total_c = float(
                penalties.get("late_total_capped", penalties.get("late_total", 0.0))
            )
            early_total_c = float(
                penalties.get("early_total_capped", penalties.get("early_total", 0.0))
            )
            ndd_total_c = float(
                penalties.get("no_due_total_capped", penalties.get("no_due_total", 0.0))
            )
            late_days = (late_total_c / lw) if lw > 0 else 0.0
            early_days = (early_total_c / ew) if ew > 0 else 0.0
            ndd_items = (ndd_total_c / ndp) if ndp > 0 else 0.0

            md_content.append("### Impact Analysis")
            md_content.append("")
            md_content.append(f"- **Late days total:** {late_days:.1f} days")
            md_content.append(
                f"- **Early days beyond tolerance:** {early_days:.1f} days"
            )
            md_content.append(f"- **Items without due date:** {ndd_items:.1f}")
            md_content.append("")

            # Impact on score versus no-penalty baseline (100%)
            avg_capped = float(penalties.get("avg_per_item_capped", 0.0) or 0.0)
            md_content.append(
                f"**Score Impact:** Potential 100.0% → Achieved {wa:.1f}% (impact: −{avg_capped:.1f} pts)"
            )
            md_content.append("")
        else:
            if total_completed > 0:
                successful_completion = early_count + on_time_count
                success_rate = (successful_completion / total_completed) * 100
                md_content.append(
                    f"**✅ Completion Success Rate:** {success_rate:.1f}%"
                )
                md_content.append(
                    f"- Successfully completed on or before due date: {successful_completion}/{total_completed} issues"
                )
                md_content.append("")
            if late_count > 0:
                md_content.append(f"- {late_count} issues completed late")
                md_content.append("")

        # Time Distribution as subsection of Performance Analysis
        time_dist = results.get("time_distribution")
        if time_dist and time_dist.get("total_considered", 0) > 0:
            md_content.append("### 📊 Time Distribution (Days from Due Date)")
            md_content.append("")

            # ASCII bar chart
            bins = time_dist.get("bins", [])
            max_count = max((b.get("count", 0) for b in bins), default=0)

            md_content.append("```")
            md_content.append(" Bucket    | Issues |  Percent | Bar")
            md_content.append(" --------- | ------ | -------- | --------------------")
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
                md_content.append(f" {label:<9} | {count:>6} | {percent_text} | {bar}")
            total_considered = time_dist.get("total_considered", 0)
            # Total line
            total_percent = "100.0%".rjust(8)
            md_content.append(f" Total     | {total_considered:>6} | {total_percent} |")
            md_content.append("```")
            md_content.append("")

        # Detailed Issue Analysis
        if issues:
            md_content.append("## 📝 Detailed Issue Analysis")
            md_content.append("")

            # Group issues by status
            status_groups: Dict[str, List[Dict]] = {}
            for issue in issues:
                status = issue.get("adherence_status", "unknown")
                if status not in status_groups:
                    status_groups[status] = []
                status_groups[status].append(issue)

            # Order statuses by priority (problems first)
            status_order = ["late", "on_time", "early", "in_progress", "no_due_date"]

            for status in status_order:
                if status not in status_groups:
                    continue

                status_issues = status_groups[status]

                # Sort issues by days_difference for better readability
                if status == "late":
                    # Late issues: worst (most late) first
                    status_issues = sorted(
                        status_issues,
                        key=lambda x: x.get("days_difference", 0),
                        reverse=True,
                    )
                elif status == "early":
                    # Early issues: most early first
                    status_issues = sorted(
                        status_issues,
                        key=lambda x: abs(x.get("days_difference", 0)),
                        reverse=True,
                    )
                elif status == "on_time":
                    # On-time: keep original order or sort by issue key
                    status_issues = sorted(
                        status_issues, key=lambda x: x.get("issue_key", "")
                    )

                emoji = self._get_adherence_status_emoji(status)
                status_label = status.replace("_", " ").title()

                md_content.append(
                    f"### {emoji} {status_label} Issues ({len(status_issues)})"
                )
                md_content.append("")

                if len(status_issues) <= 10:  # Show all if 10 or fewer
                    md_content.append(
                        "| Issue Key | Summary | Type | Assignee | Days Difference |"
                    )
                    md_content.append(
                        "|-----------|---------|------|-----------|-----------------|"
                    )

                    for issue in status_issues:
                        issue_key = issue.get("issue_key", "N/A")
                        summary = issue.get("summary", "N/A")[:50] + (
                            "..." if len(issue.get("summary", "")) > 50 else ""
                        )
                        issue_type = issue.get("issue_type", "N/A")
                        assignee = issue.get("assignee", "Unassigned")
                        days_diff = issue.get("days_difference")

                        if days_diff is not None:
                            if status == "early":
                                days_text = f"{abs(days_diff)} days early"
                            elif status == "late":
                                days_text = f"{days_diff} days late"
                            elif status == "on_time":
                                days_text = "On time"
                            else:
                                days_text = "N/A"
                        else:
                            days_text = "N/A"

                        md_content.append(
                            f"| {issue_key} | {summary} | {issue_type} | {assignee} | {days_text} |"
                        )
                else:
                    # Show summary for large lists
                    md_content.append(
                        f"**{len(status_issues)} issues** in this category. Top 5 by impact:"
                    )
                    md_content.append("")
                    md_content.append(
                        "| Issue Key | Summary | Type | Days Difference |"
                    )
                    md_content.append(
                        "|-----------|---------|------|-----------------|"
                    )

                    # Use already sorted issues (top 5 by impact)
                    for issue in status_issues[:5]:
                        issue_key = issue.get("issue_key", "N/A")
                        summary = issue.get("summary", "N/A")[:40] + (
                            "..." if len(issue.get("summary", "")) > 40 else ""
                        )
                        issue_type = issue.get("issue_type", "N/A")
                        days_diff = issue.get("days_difference")

                        if days_diff is not None:
                            if status == "early":
                                days_text = f"{abs(days_diff)} early"
                            elif status == "late":
                                days_text = f"{days_diff} late"
                            else:
                                days_text = "On time"
                        else:
                            days_text = "N/A"

                        md_content.append(
                            f"| {issue_key} | {summary} | {issue_type} | {days_text} |"
                        )

                md_content.append("")

        # Enhanced Recommendations based on multiple factors
        md_content.append("## 💡 Recommendations")
        md_content.append("")

        # General adherence recommendations
        if adherence_rate < 60:
            md_content.append("### 🚨 Immediate Actions Required")
            md_content.append("- Review project planning and estimation processes")
            md_content.append("- Implement more frequent milestone check-ins")
            md_content.append("- Consider workload balancing across team members")
        elif adherence_rate < 80:
            md_content.append("### ⚠️ Improvement Opportunities")
            md_content.append("- Enhance due date visibility and tracking")
            md_content.append("- Implement early warning systems for at-risk issues")
            md_content.append("- Review resource allocation and capacity planning")
        else:
            md_content.append("### ✅ Maintain Excellence")
            md_content.append("- Continue current practices and monitoring")
            md_content.append("- Share best practices with other teams")
            md_content.append("- Focus on continuous improvement")
        md_content.append("")

        # Specific recommendations based on analysis
        specific_recommendations = []

        # Due date coverage recommendations
        if (
            due_date_coverage
            and due_date_coverage.get("overall", {}).get("coverage_percentage", 0) < 80
        ):
            specific_recommendations.append(
                "**Due Date Coverage:** Enforce due date requirements - only {:.1f}% of issues have due dates set".format(
                    due_date_coverage.get("overall", {}).get("coverage_percentage", 0)
                )
            )

        # Statistical outlier recommendations
        if (
            statistical_insights
            and statistical_insights.get("outlier_analysis", {}).get(
                "outlier_percentage", 0
            )
            > 10
        ):
            outlier_pct = statistical_insights.get("outlier_analysis", {}).get(
                "outlier_percentage", 0
            )
            specific_recommendations.append(
                f"**Outlier Management:** {outlier_pct:.1f}% of issues are outliers - investigate extreme cases for process improvements"
            )

        # Segmentation-based recommendations
        if segmentation:
            # Find worst performing teams (only if analyzing multiple teams)
            by_team = segmentation.get("by_team", {})
            # Only show team recommendations if we're analyzing multiple teams
            if len(by_team) > 1:
                worst_teams = [
                    (team, metrics)
                    for team, metrics in by_team.items()
                    if metrics.get("adherence_rate", 0) < 60
                    and metrics.get("total_completed", 0) >= 3
                ]
                if worst_teams:
                    team_names = [team for team, _ in worst_teams[:2]]
                    specific_recommendations.append(
                        f"**Team Focus:** Teams requiring immediate support: {', '.join(team_names)}"
                    )

        # Weighted scoring recommendations
        if weighted_perf:
            penalties = weighted_perf.get("penalties", {})
            late_share = (
                penalties.get("late_total_capped", 0.0)
                / max(1, penalties.get("total_capped", 1))
            ) * 100
            early_share = (
                penalties.get("early_total_capped", 0.0)
                / max(1, penalties.get("total_capped", 1))
            ) * 100
            ndd_share = (
                penalties.get("no_due_total_capped", 0.0)
                / max(1, penalties.get("total_capped", 1))
            ) * 100

            if late_share > 50:
                specific_recommendations.append(
                    f"**Lateness Focus:** {late_share:.0f}% of penalties from late delivery - strengthen deadline management"
                )
            if ndd_share > 30:
                specific_recommendations.append(
                    f"**Due Date Governance:** {ndd_share:.0f}% of penalties from missing due dates - enforce planning standards"
                )
            if early_share > 30:
                specific_recommendations.append(
                    f"**Estimation Accuracy:** {early_share:.0f}% of penalties from early delivery - improve estimation and milestone alignment"
                )

        if specific_recommendations:
            md_content.append("### 🎯 Specific Action Items")
            md_content.append("")
            for rec in specific_recommendations:
                md_content.append(f"- {rec}")
            md_content.append("")

        # Data Quality Notes
        md_content.append("## 📋 Data Quality & Methodology")
        md_content.append("")

        md_content.append("### Data Scope")
        md_content.append(
            f"- **Analysis Period:** {metadata.get('time_period', 'Unknown')}"
        )
        md_content.append(
            f"- **Date Range:** {start_date[:10] if start_date else 'Unknown'} to {end_date[:10] if end_date else 'Unknown'}"
        )
        md_content.append(f"- **Issue Types:** {', '.join(issue_types)}")
        if team:
            md_content.append(f"- **Team Filter:** {team}")
        md_content.append(
            f"- **Schema Version:** {results.get('schema_version', '1.0')}"
        )
        md_content.append("")

        md_content.append("### Methodology")
        md_content.append(
            "- **Legacy Adherence:** (Early + On-time) / Completed issues"
        )
        md_content.append(
            "- **Weighted Adherence:** Averages per-issue scores (0-100%) with asymmetric penalties"
        )
        md_content.append("  - Late completion: Higher penalty weight, no tolerance")
        md_content.append(
            "  - Early completion: Lower penalty weight, tolerance period applied"
        )
        md_content.append(
            "  - Progressive penalties: Severe lateness (>7 days) gets escalating penalties"
        )
        md_content.append(
            "  - Missing due dates: Fixed penalty when included in analysis"
        )
        md_content.append(
            "- **Statistical Analysis:** Uses IQR method for outlier detection (1.5×IQR)"
        )
        md_content.append(
            "- **Timestamps:** Based on JIRA resolution dates and due dates (timezone-normalized)"
        )
        md_content.append("")

        # Explicit list of issues without due date at the end if present
        no_due_list = results.get("issues_without_due_date", [])
        if no_due_list and len(no_due_list) <= 20:  # Only show if manageable list
            md_content.append("## 📎 Issues Without Due Date")
            md_content.append("")
            md_content.append("| Issue Key | Team | Assignee | Summary |")
            md_content.append("|-----------|------|----------|---------|")
            for item in no_due_list:
                issue_key = item.get("issue_key", "N/A")
                team_name = item.get("team") or "Not set"
                assignee = item.get("assignee") or "Unassigned"
                summary = (item.get("summary") or "").strip()
                summary_short = (
                    (summary[:50] + ("..." if len(summary) > 50 else ""))
                    if summary
                    else ""
                )
                md_content.append(
                    f"| {issue_key} | {team_name} | {assignee} | {summary_short} |"
                )
            md_content.append("")
        elif len(no_due_list) > 20:
            md_content.append("## 📎 Issues Without Due Date")
            md_content.append("")
            md_content.append(
                f"**{len(no_due_list)} issues without due dates** (showing summary by team)"
            )
            md_content.append("")

            # Group by team and show summary
            team_counts = {}
            for item in no_due_list:
                team_name = item.get("team") or "Not set"
                team_counts[team_name] = team_counts.get(team_name, 0) + 1

            md_content.append("| Team | Count |")
            md_content.append("|------|-------|")
            for team_name, count in sorted(
                team_counts.items(), key=lambda x: x[1], reverse=True
            ):
                md_content.append(f"| {team_name} | {count} |")
            md_content.append("")

        # Footer with generation info
        md_content.append("---")
        md_content.append("")
        md_content.append(
            f"*Report generated on {analysis_date[:19] if analysis_date else datetime.now().isoformat()[:19]} using PyToolkit JIRA Adherence Analysis*"
        )
        md_content.append("")

        return "\n".join(md_content)

    def _calculate_statistical_insights(
        self, results: List[IssueAdherenceResult]
    ) -> Dict:
        """Calculate statistical insights for completion times."""
        completed_with_due_dates = [
            r
            for r in results
            if r.adherence_status in ("early", "on_time", "late")
            and r.days_difference is not None
        ]

        if not completed_with_due_dates:
            return {
                "total_completed_with_due_dates": 0,
                "delivery_time_stats": {},
                "percentile_analysis": {},
                "outlier_analysis": {},
            }

        days_differences = [r.days_difference for r in completed_with_due_dates]

        # Basic statistics
        delivery_stats = {
            "mean": round(statistics.mean(days_differences), 2),
            "median": round(statistics.median(days_differences), 2),
            "std_dev": round(statistics.stdev(days_differences), 2)
            if len(days_differences) > 1
            else 0.0,
            "min": min(days_differences),
            "max": max(days_differences),
            "range": max(days_differences) - min(days_differences),
        }

        # Percentile analysis
        percentiles = [10, 25, 50, 75, 90, 95]
        percentile_analysis = {}
        for p in percentiles:
            try:
                value = (
                    statistics.quantiles(days_differences, n=100)[p - 1]
                    if len(days_differences) >= 2
                    else days_differences[0]
                )
                percentile_analysis[f"p{p}"] = round(value, 2)
            except (IndexError, statistics.StatisticsError):
                percentile_analysis[f"p{p}"] = (
                    days_differences[0] if days_differences else 0
                )

        # Outlier analysis (using IQR method)
        if len(days_differences) >= 4:
            q1 = percentile_analysis.get("p25", 0)
            q3 = percentile_analysis.get("p75", 0)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr

            outliers = [
                d for d in days_differences if d < lower_bound or d > upper_bound
            ]
            outlier_analysis = {
                "outlier_count": len(outliers),
                "outlier_percentage": round(
                    len(outliers) / len(days_differences) * 100, 1
                ),
                "extreme_early": [d for d in outliers if d < lower_bound],
                "extreme_late": [d for d in outliers if d > upper_bound],
                "iqr": round(iqr, 2),
                "lower_bound": round(lower_bound, 2),
                "upper_bound": round(upper_bound, 2),
            }
        else:
            outlier_analysis = {
                "outlier_count": 0,
                "outlier_percentage": 0.0,
                "extreme_early": [],
                "extreme_late": [],
                "iqr": 0.0,
                "lower_bound": 0.0,
                "upper_bound": 0.0,
            }

        return {
            "total_completed_with_due_dates": len(completed_with_due_dates),
            "delivery_time_stats": delivery_stats,
            "percentile_analysis": percentile_analysis,
            "outlier_analysis": outlier_analysis,
        }

    def _calculate_segmentation_analysis(
        self, results: List[IssueAdherenceResult]
    ) -> Dict:
        """Calculate adherence metrics segmented by team, issue type, and assignee."""
        segments = {
            "by_team": defaultdict(
                lambda: {
                    "early": 0,
                    "on_time": 0,
                    "late": 0,
                    "no_due_date": 0,
                    "in_progress": 0,
                }
            ),
            "by_issue_type": defaultdict(
                lambda: {
                    "early": 0,
                    "on_time": 0,
                    "late": 0,
                    "no_due_date": 0,
                    "in_progress": 0,
                }
            ),
            "by_assignee": defaultdict(
                lambda: {
                    "early": 0,
                    "on_time": 0,
                    "late": 0,
                    "no_due_date": 0,
                    "in_progress": 0,
                }
            ),
        }

        for result in results:
            status = result.adherence_status

            # Segment by team
            team = result.team or "Unassigned"
            segments["by_team"][team][status] += 1

            # Segment by issue type
            issue_type = result.issue_type or "Unknown"
            segments["by_issue_type"][issue_type][status] += 1

            # Segment by assignee
            assignee = result.assignee or "Unassigned"
            segments["by_assignee"][assignee][status] += 1

        # Calculate adherence rates for each segment
        def calculate_segment_metrics(segment_data):
            metrics = {}
            for key, counts in segment_data.items():
                early = counts["early"]
                on_time = counts["on_time"]
                late = counts["late"]
                total_completed = early + on_time + late

                if total_completed > 0:
                    adherence_rate = (early + on_time) / total_completed * 100
                else:
                    adherence_rate = 0.0

                metrics[key] = {
                    "counts": dict(counts),
                    "total_completed": total_completed,
                    "total_issues": sum(counts.values()),
                    "adherence_rate": round(adherence_rate, 1),
                }

            # Sort by adherence rate (worst first) for actionable insights
            return dict(sorted(metrics.items(), key=lambda x: x[1]["adherence_rate"]))

        return {
            "by_team": calculate_segment_metrics(segments["by_team"]),
            "by_issue_type": calculate_segment_metrics(segments["by_issue_type"]),
            "by_assignee": calculate_segment_metrics(segments["by_assignee"]),
        }

    def _calculate_due_date_coverage_analysis(
        self, all_issues: List[Dict], analyzed_results: List[IssueAdherenceResult]
    ) -> Dict:
        """Analyze due date coverage across the dataset."""
        total_issues = len(all_issues)

        # Count issues with and without due dates from original dataset
        issues_with_due_dates = 0
        issues_without_due_dates = 0

        for issue in all_issues:
            fields = issue.get("fields", {})
            due_date = fields.get("duedate")
            if due_date:
                issues_with_due_dates += 1
            else:
                issues_without_due_dates += 1

        # Calculate coverage percentage
        due_date_coverage_percentage = (
            (issues_with_due_dates / total_issues * 100) if total_issues > 0 else 0.0
        )

        # Analyze by team and issue type
        team_coverage = defaultdict(lambda: {"with_due_date": 0, "without_due_date": 0})
        type_coverage = defaultdict(lambda: {"with_due_date": 0, "without_due_date": 0})

        for issue in all_issues:
            fields = issue.get("fields", {})
            due_date = fields.get("duedate")
            team = (
                fields.get("customfield_10265", {}).get("value")
                if fields.get("customfield_10265")
                else "Unassigned"
            )
            issue_type = fields.get("issuetype", {}).get("name", "Unknown")

            coverage_key = "with_due_date" if due_date else "without_due_date"
            team_coverage[team][coverage_key] += 1
            type_coverage[issue_type][coverage_key] += 1

        # Calculate coverage rates by segment
        def calculate_coverage_rates(coverage_data):
            rates = {}
            for key, counts in coverage_data.items():
                total = counts["with_due_date"] + counts["without_due_date"]
                coverage_rate = (
                    (counts["with_due_date"] / total * 100) if total > 0 else 0.0
                )
                rates[key] = {
                    "with_due_date": counts["with_due_date"],
                    "without_due_date": counts["without_due_date"],
                    "total": total,
                    "coverage_rate": round(coverage_rate, 1),
                }
            # Sort by coverage rate (worst first)
            return dict(sorted(rates.items(), key=lambda x: x[1]["coverage_rate"]))

        return {
            "overall": {
                "total_issues": total_issues,
                "issues_with_due_dates": issues_with_due_dates,
                "issues_without_due_dates": issues_without_due_dates,
                "coverage_percentage": round(due_date_coverage_percentage, 1),
            },
            "by_team": calculate_coverage_rates(team_coverage),
            "by_issue_type": calculate_coverage_rates(type_coverage),
        }

    def _calculate_delivery_time_distribution(
        self, results: List[IssueAdherenceResult]
    ) -> Dict:
        """
        Compute distribution of days early/late (integers) for completed items with due dates.

        Bins (contiguous, non-overlapping for integer days):
          < -10, -10 to -5, -5 to -3, -3 to -1, -1, 0, 1, 2 to 3, 4 to 5, 6 to 10, > 10

        Percentages are relative to total completed items with due dates considered.
        """
        order = [
            "< -10",
            "-10 to -5",
            "-5 to -3",
            "-3 to -1",
            "-1",
            "0",
            "1",
            "2 to 3",
            "4 to 5",
            "6 to 10",
            "> 10",
        ]

        bins: Dict[str, int] = {k: 0 for k in order}
        total_considered = 0

        for r in results:
            if r.adherence_status not in ("early", "on_time", "late"):
                continue
            if r.days_difference is None:
                continue

            d = int(r.days_difference)
            # Completed items with due date are considered
            total_considered += 1

            if d <= -11:
                bins["< -10"] += 1
            elif -10 <= d <= -6:
                bins["-10 to -5"] += 1
            elif -5 <= d <= -4:
                bins["-5 to -3"] += 1
            elif -3 <= d <= -2:
                bins["-3 to -1"] += 1
            elif d == -1:
                bins["-1"] += 1
            elif d == 0:
                bins["0"] += 1
            elif d == 1:
                bins["1"] += 1
            elif 2 <= d <= 3:
                bins["2 to 3"] += 1
            elif 4 <= d <= 5:
                bins["4 to 5"] += 1
            elif 6 <= d <= 10:
                bins["6 to 10"] += 1
            else:  # d >= 11
                bins["> 10"] += 1

        def pct(x: int) -> float:
            return (
                round((x / total_considered * 100.0), 1)
                if total_considered > 0
                else 0.0
            )

        return {
            "total_considered": total_considered,
            "bins": [
                {"label": key, "count": bins[key], "percentage": pct(bins[key])}
                for key in order
            ],
        }
