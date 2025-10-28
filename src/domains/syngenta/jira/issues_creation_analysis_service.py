"""
JIRA Issues Creation Analysis Service

This service provides functionality to analyze issues creation patterns over time by fetching
issues created within specified time periods and calculating aggregated statistics by
time periods, issue types, and projects.
"""

import csv
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

from domains.syngenta.jira.issue_adherence_service import TimePeriodParser
from utils.cache_manager.cache_manager import CacheManager
from utils.data.json_manager import JSONManager
from utils.jira.error import JiraQueryError
from utils.jira.jira_assistant import JiraAssistant
from utils.logging.logging_manager import LogManager
from utils.output_manager import OutputManager


@dataclass
class IssueCreationResult:
    """Data class to hold individual issue creation data."""

    issue_key: str
    summary: str
    issue_type: str
    priority: str
    status: str
    assignee: Optional[str]
    project_key: str
    labels: List[str]
    created_date: str
    created_date_parsed: date


class IssuesCreationAnalysisService:
    """Service for analyzing JIRA issues creation patterns over time."""

    def __init__(self, cache_expiration: int = 60):
        """
        Initialize the service.

        Args:
            cache_expiration (int): Cache expiration time in minutes (default: 60)
        """
        self.jira_assistant = JiraAssistant(cache_expiration=cache_expiration)
        self.cache_manager = CacheManager.get_instance()
        self.cache_expiration = cache_expiration
        self.logger = LogManager.get_instance().get_logger("IssuesCreationAnalysisService")
        self.time_parser = TimePeriodParser()

    def clear_cache(self):
        """Clear all cached data for this service."""
        self.cache_manager.clear_all()
        self.logger.info("Cache cleared successfully")

    def analyze_issues_creation(
        self,
        project_key: str,
        start_date: date,
        end_date: date,
        aggregation: str = "daily",
        issue_types: Optional[List[str]] = None,
        teams: Optional[List[str]] = None,
        labels: Optional[List[str]] = None,
        additional_jql: Optional[str] = None,
        include_summary: bool = False,
        output_file: Optional[str] = None,
        output_format: str = "console",
        verbose: bool = False,
    ) -> Dict:
        """
        Analyze issues creation patterns over time.

        Args:
            project_key: JIRA project key to analyze
            start_date: Start date for analysis
            end_date: End date for analysis
            aggregation: Aggregation level - daily, weekly, or monthly
            issue_types: List of issue types to filter
            teams: List of team names to filter (Squad[Dropdown] field)
            labels: List of labels to filter
            additional_jql: Additional JQL filter
            include_summary: Whether to include summary statistics
            output_file: Output file path for results
            output_format: Output format (json, md, console)
            verbose: Enable verbose output with date-by-date logging

        Returns:
            Dict containing analysis results with aggregated data
        """
        try:
            self.logger.info(
                f"Analyzing issues creation from {start_date} to {end_date} with {aggregation} aggregation"
            )

            # Build JQL query
            jql_query = self._build_jql_query(
                project_key, start_date, end_date, issue_types, teams, labels, additional_jql
            )

            # Fetch issues with pagination to get all results
            self.logger.info(f"Executing JQL query: {jql_query}")
            # Fetch issues with pagination to get all results
            # Our fetch_issues method handles pagination automatically to get all results
            issues = self.jira_assistant.fetch_issues(
                jql_query=jql_query,
                fields="key,summary,created,issuetype,project,labels,status,assignee,priority",
                max_results=100,  # Use 100 to work with JIRA API limitations
                expand_changelog=False,
            )

            self.logger.info(f"Fetched {len(issues)} total issues from JIRA")

            # Log some debugging info about the issues
            if issues:
                self.logger.info(f"First issue key: {issues[0].get('key', 'N/A')}")
                self.logger.info(f"Last issue key: {issues[-1].get('key', 'N/A')}")

                # Check issue types distribution
                issue_types_count = {}
                for issue in issues:
                    issue_type = issue.get("fields", {}).get("issuetype", {}).get("name", "Unknown")
                    issue_types_count[issue_type] = issue_types_count.get(issue_type, 0) + 1
                self.logger.info(f"Issue types distribution: {issue_types_count}")

                # Check project distribution
                project_count = {}
                for issue in issues:
                    project_key = issue.get("fields", {}).get("project", {}).get("key", "Unknown")
                    project_count[project_key] = project_count.get(project_key, 0) + 1
                self.logger.info(f"Project distribution: {project_count}")

                # Check date range of fetched issues
                creation_dates = []
                for issue in issues:
                    created_date = issue.get("fields", {}).get("created")
                    if created_date:
                        creation_dates.append(created_date)

                if creation_dates:
                    creation_dates.sort()
                    self.logger.info(f"Date range: {creation_dates[0]} to {creation_dates[-1]}")

            # Process issues into structured results
            creation_results = []
            issues_with_missing_data = 0
            all_assignees = set()
            all_labels = set()

            for issue in issues:
                result = self._process_issue_creation(issue)
                if result:
                    creation_results.append(result)
                    # Collect debugging information
                    if result.assignee:
                        all_assignees.add(result.assignee)
                    for label in result.labels:
                        all_labels.add(label)
                else:
                    issues_with_missing_data += 1

            self.logger.info(f"Processed {len(creation_results)} valid issues")
            self.logger.info(f"Issues with missing data: {issues_with_missing_data}")

            if creation_results:
                self.logger.info(f"Unique assignees found: {len(all_assignees)}")
                self.logger.info(f"Unique labels found: {len(all_labels)}")

                # Log date range of processed issues
                processed_dates = [r.created_date_parsed for r in creation_results]
                processed_dates.sort()
                self.logger.info(f"Processed date range: {processed_dates[0]} to {processed_dates[-1]}")

            # Aggregate data by time period with complete timeline
            self.logger.info(f"Aggregating data by {aggregation} from {start_date} to {end_date}")
            aggregated_data = self._aggregate_issues_by_time(
                creation_results, aggregation, start_date, end_date, verbose
            )

            # Log aggregation results
            periods_with_issues = len([d for d in aggregated_data if d["total_issues"] > 0])
            self.logger.info(
                f"Generated {len(aggregated_data)} {aggregation} periods, {periods_with_issues} with issues"
            )

            if aggregated_data:
                issue_counts = [d["total_issues"] for d in aggregated_data]
                max_issues = max(issue_counts)
                avg_issues = sum(issue_counts) / len(issue_counts)
                self.logger.info(f"Issues per period - Max: {max_issues}, Avg: {avg_issues:.1f}")

            # Build comprehensive result structure
            result = {
                "query_info": {
                    "project_key": project_key,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "window_days": (end_date - start_date).days,
                    "aggregation": aggregation,
                    "issue_types": issue_types,
                    "teams": teams,
                    "labels": labels,
                    "additional_jql": additional_jql,
                    "jql_query": jql_query,
                },
                "summary": {
                    "total_issues": len(creation_results),
                    "analysis_period_days": (end_date - start_date).days + 1,
                    "total_periods": len(aggregated_data),
                    "periods_with_issues": len([d for d in aggregated_data if d["total_issues"] > 0]),
                },
                # Add analysis_metadata for JiraSummaryManager compatibility
                "analysis_metadata": {
                    "project": project_key,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "teams": teams,
                    "aggregation": aggregation,
                },
                # Add metrics for JiraSummaryManager compatibility
                "metrics": {
                    "total_issues": len(creation_results),
                    "analysis_period_days": (end_date - start_date).days + 1,
                    "total_periods": len(aggregated_data),
                    "periods_with_issues": len([d for d in aggregated_data if d["total_issues"] > 0]),
                },
                "data": aggregated_data,
                "issues": [self._result_to_dict(r) for r in creation_results],
            }

            # Add comprehensive summary if requested
            if include_summary:
                result["detailed_summary"] = self._generate_comprehensive_summary(
                    creation_results, aggregated_data, aggregation
                )

            # Save results based on output format
            if output_format in ["json", "md"]:
                if output_file:
                    # Use specified output file
                    if output_format == "md":
                        self._save_markdown_report(result, output_file)
                    else:
                        self._save_results(result, output_file)
                    result["output_file"] = output_file
                    self.logger.info(f"Results saved to specified file: {output_file}")
                else:
                    # Generate default output file using OutputManager
                    extension = "md" if output_format == "md" else "json"
                    output_path = OutputManager.get_output_path(
                        sub_dir="issues-creation-analysis", file_name=f"analysis_{aggregation}", extension=extension
                    )
                    if output_format == "md":
                        self._save_markdown_report(result, output_path)
                    else:
                        self._save_results(result, output_path)
                    result["output_file"] = output_path
                    self.logger.info(f"Results saved to default location: {output_path}")
            else:
                # Console output - still save JSON for summary metrics
                output_path = OutputManager.get_output_path(
                    sub_dir="issues-creation-analysis", file_name=f"analysis_{aggregation}"
                )
                self._save_results(result, output_path)
                result["output_file"] = output_path
                self.logger.info(f"Results saved for metrics: {output_path}")

            return result

        except Exception as e:
            self.logger.error(f"Error analyzing issues creation: {e}", exc_info=True)
            raise JiraQueryError("Failed to analyze issues creation", error=str(e)) from e

    def _build_jql_query(
        self,
        project_key: str,
        start_date: date,
        end_date: date,
        issue_types: Optional[List[str]] = None,
        teams: Optional[List[str]] = None,
        labels: Optional[List[str]] = None,
        additional_jql: Optional[str] = None,
    ) -> str:
        """
        Build JQL query with all filters.

        Args:
            project_key: Project key to filter
            start_date: Start date for filtering
            end_date: End date for filtering
            issue_types: Issue types to filter
            teams: Team names to filter (Squad[Dropdown] field)
            labels: Labels to filter
            additional_jql: Additional JQL conditions

        Returns:
            Complete JQL query string
        """
        jql_parts = []

        # Project filter (required)
        jql_parts.append(f"project = '{project_key}'")

        # Date range filter for created date
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        date_filter = f"created >= '{start_str}' AND created <= '{end_str} 23:59'"
        jql_parts.append(date_filter)
        self.logger.debug(f"Date filter: {date_filter}")

        # Issue types filter
        if issue_types:
            if len(issue_types) == 1:
                type_filter = f"type = '{issue_types[0]}'"
                jql_parts.append(type_filter)
                self.logger.debug(f"Issue type filter: {type_filter}")
            else:
                types_str = "', '".join(issue_types)
                type_filter = f"type IN ('{types_str}')"
                jql_parts.append(type_filter)
                self.logger.debug(f"Issue types filter: {type_filter}")

        # Teams filter (using Squad[Dropdown] field)
        if teams:
            if len(teams) == 1:
                team_filter = f"'Squad[Dropdown]' = '{teams[0]}'"
                jql_parts.append(team_filter)
                self.logger.debug(f"Team filter: {team_filter}")
            else:
                teams_str = "', '".join(teams)
                team_filter = f"'Squad[Dropdown]' IN ('{teams_str}')"
                jql_parts.append(team_filter)
                self.logger.debug(f"Teams filter: {team_filter}")

        # Labels filter
        if labels:
            for label in labels:
                label_filter = f"labels = '{label}'"
                jql_parts.append(label_filter)
                self.logger.debug(f"Label filter: {label_filter}")

        # Additional JQL
        if additional_jql:
            additional_filter = f"({additional_jql})"
            jql_parts.append(additional_filter)
            self.logger.debug(f"Additional JQL filter: {additional_filter}")

        # Combine all conditions
        jql_query = " AND ".join(jql_parts)

        # Add ordering for consistent results
        jql_query += " ORDER BY created ASC"

        self.logger.debug(f"Final JQL query: {jql_query}")
        return jql_query

    def _process_issue_creation(self, issue: Dict) -> Optional[IssueCreationResult]:
        """
        Process a single issue into structured creation result.

        Args:
            issue: Raw JIRA issue data

        Returns:
            IssueCreationResult or None if processing fails
        """
        try:
            fields = issue.get("fields", {})

            # Extract basic issue information
            issue_key = issue.get("key", "")
            summary = fields.get("summary", "")
            issue_type = fields.get("issuetype", {}).get("name", "")
            priority = fields.get("priority", {}).get("name", "Unknown")
            status = fields.get("status", {}).get("name", "")
            project_key = fields.get("project", {}).get("key", "")

            # Extract assignee
            assignee_field = fields.get("assignee")
            assignee = assignee_field.get("displayName") if assignee_field else None

            # Extract labels
            labels_field = fields.get("labels", [])
            labels = list(labels_field) if labels_field else []

            # Extract and parse created date
            created_date = fields.get("created")
            if not created_date:
                self.logger.warning(f"Issue {issue_key} has no created date")
                return None

            # Parse created date
            created_date_parsed = datetime.fromisoformat(created_date.replace("Z", "+00:00")).date()

            return IssueCreationResult(
                issue_key=issue_key,
                summary=summary,
                issue_type=issue_type,
                priority=priority,
                status=status,
                assignee=assignee,
                project_key=project_key,
                labels=labels,
                created_date=created_date,
                created_date_parsed=created_date_parsed,
            )

        except Exception as e:
            self.logger.warning(f"Error processing issue {issue.get('key', 'unknown')}: {e}")
            return None

    def _aggregate_issues_by_time(
        self,
        issues: List[IssueCreationResult],
        aggregation: str,
        start_date: date,
        end_date: date,
        verbose: bool = False,
    ) -> List[Dict]:
        """
        Aggregate issues by time period, filling in missing periods with zero counts.

        Args:
            issues: List of IssueCreationResult objects
            aggregation: Aggregation level (daily, weekly, monthly)
            start_date: Start date for complete timeline
            end_date: End date for complete timeline
            verbose: Enable verbose date-by-date logging

        Returns:
            List of aggregated data points with complete timeline
        """
        # Group issues by time period
        time_groups = defaultdict(list)

        for issue in issues:
            created_date = issue.created_date_parsed

            # Determine time period key based on aggregation
            if aggregation == "daily":
                period_key = created_date.isoformat()
            elif aggregation == "weekly":
                # Get start of week (Monday)
                start_of_week = created_date - timedelta(days=created_date.weekday())
                period_key = start_of_week.isoformat()
            elif aggregation == "monthly":
                # Get start of month
                start_of_month = created_date.replace(day=1)
                period_key = start_of_month.isoformat()
            else:
                raise ValueError(f"Unsupported aggregation: {aggregation}")

            time_groups[period_key].append(issue)

        # Generate complete timeline with missing periods filled
        complete_timeline = self._generate_complete_timeline(start_date, end_date, aggregation)

        # Convert to sorted list of data points
        aggregated_data = []
        for period_key in complete_timeline:
            issues_in_period = time_groups.get(period_key, [])

            # Calculate detailed statistics for the period
            period_stats = self._calculate_period_statistics(issues_in_period)

            data_point = {
                "period": period_key,
                "total_issues": len(issues_in_period),
                "issue_types": period_stats["issue_types"],
                "projects": period_stats["projects"],
                "priorities": period_stats["priorities"],
                "assignees": period_stats["assignees"],
                "labels": period_stats["labels"],
                "issues": [self._result_to_dict(issue) for issue in issues_in_period],
            }

            aggregated_data.append(data_point)

            # Verbose logging: log each period with its details
            if verbose:
                issue_count = len(issues_in_period)
                if issue_count > 0:
                    # Log period with issues
                    issue_keys = [issue.issue_key for issue in issues_in_period]
                    issue_types_str = ", ".join(f"{k}: {v}" for k, v in period_stats["issue_types"].items())
                    projects_str = ", ".join(f"{k}: {v}" for k, v in period_stats["projects"].items())

                    self.logger.info(f"📅 {period_key}: {issue_count} issues")
                    self.logger.info(f"   Issue keys: {', '.join(issue_keys)}")
                    self.logger.info(f"   Issue types: {issue_types_str}")
                    self.logger.info(f"   Projects: {projects_str}")

                    if period_stats["assignees"]:
                        assignees_str = ", ".join(f"{k}: {v}" for k, v in period_stats["assignees"].items())
                        self.logger.info(f"   Assignees: {assignees_str}")
                else:
                    # Log period with no issues
                    self.logger.info(f"📅 {period_key}: 0 issues")

        return aggregated_data

    def _generate_complete_timeline(self, start_date: date, end_date: date, aggregation: str) -> List[str]:
        """
        Generate a complete timeline with all periods between start and end dates.

        Args:
            start_date: Start date
            end_date: End date
            aggregation: Aggregation level (daily, weekly, monthly)

        Returns:
            List of period keys in chronological order
        """
        timeline = []
        current_date = start_date

        while current_date <= end_date:
            if aggregation == "daily":
                timeline.append(current_date.isoformat())
                current_date += timedelta(days=1)
            elif aggregation == "weekly":
                # Get start of week (Monday)
                start_of_week = current_date - timedelta(days=current_date.weekday())
                period_key = start_of_week.isoformat()
                if period_key not in timeline:
                    timeline.append(period_key)
                current_date += timedelta(days=7)
            elif aggregation == "monthly":
                # Get start of month
                start_of_month = current_date.replace(day=1)
                period_key = start_of_month.isoformat()
                if period_key not in timeline:
                    timeline.append(period_key)
                # Move to next month
                if current_date.month == 12:
                    current_date = current_date.replace(year=current_date.year + 1, month=1)
                else:
                    current_date = current_date.replace(month=current_date.month + 1)
            else:
                raise ValueError(f"Unsupported aggregation: {aggregation}")

        return sorted(timeline)

    def _calculate_period_statistics(self, issues: List[IssueCreationResult]) -> Dict:
        """
        Calculate detailed statistics for a specific time period.

        Args:
            issues: List of issues in the period

        Returns:
            Dictionary with various statistics
        """
        issue_type_counts = defaultdict(int)
        project_counts = defaultdict(int)
        priority_counts = defaultdict(int)
        assignee_counts = defaultdict(int)
        label_counts = defaultdict(int)

        for issue in issues:
            issue_type_counts[issue.issue_type] += 1
            project_counts[issue.project_key] += 1
            priority_counts[issue.priority] += 1

            if issue.assignee:
                assignee_counts[issue.assignee] += 1
            else:
                assignee_counts["Unassigned"] += 1

            for label in issue.labels:
                label_counts[label] += 1

        return {
            "issue_types": dict(issue_type_counts),
            "projects": dict(project_counts),
            "priorities": dict(priority_counts),
            "assignees": dict(assignee_counts),
            "labels": dict(label_counts),
        }

    def _generate_comprehensive_summary(
        self,
        issues: List[IssueCreationResult],
        aggregated_data: List[Dict],
        aggregation: str,
    ) -> Dict:
        """
        Generate comprehensive summary statistics.

        Args:
            issues: Original issues list
            aggregated_data: Aggregated data points
            aggregation: Aggregation level

        Returns:
            Comprehensive summary statistics dictionary
        """
        if not aggregated_data:
            return {
                "total_issues": 0,
                "average_per_period": 0,
                "max_per_period": 0,
                "min_per_period": 0,
                "periods_with_issues": 0,
                "trend_analysis": {"direction": "stable", "confidence": "low"},
            }

        # Calculate basic statistics
        issue_counts = [data["total_issues"] for data in aggregated_data]

        # Overall distribution analysis
        issue_type_totals = defaultdict(int)
        project_totals = defaultdict(int)
        priority_totals = defaultdict(int)
        assignee_totals = defaultdict(int)
        label_totals = defaultdict(int)

        for issue in issues:
            issue_type_totals[issue.issue_type] += 1
            project_totals[issue.project_key] += 1
            priority_totals[issue.priority] += 1

            if issue.assignee:
                assignee_totals[issue.assignee] += 1
            else:
                assignee_totals["Unassigned"] += 1

            for label in issue.labels:
                label_totals[label] += 1

        # Advanced trend analysis
        trend_analysis = self._analyze_trends(issue_counts, aggregation)

        # Peak analysis
        peak_analysis = self._analyze_peaks(aggregated_data)

        # Distribution analysis
        distribution_analysis = self._analyze_distribution(issue_counts)

        return {
            "total_issues": len(issues),
            "total_periods": len(aggregated_data),
            "periods_with_issues": len([d for d in aggregated_data if d["total_issues"] > 0]),
            "average_per_period": sum(issue_counts) / len(issue_counts),
            "max_per_period": max(issue_counts),
            "min_per_period": min(issue_counts),
            "median_per_period": sorted(issue_counts)[len(issue_counts) // 2],
            "issue_type_distribution": dict(issue_type_totals),
            "project_distribution": dict(project_totals),
            "priority_distribution": dict(priority_totals),
            "assignee_distribution": dict(assignee_totals),
            "label_distribution": dict(label_totals),
            "trend_analysis": trend_analysis,
            "peak_analysis": peak_analysis,
            "distribution_analysis": distribution_analysis,
        }

    def _analyze_trends(self, issue_counts: List[int], aggregation_type: str) -> Dict:
        """Analyze trends in issue creation over time."""
        if len(issue_counts) < 3:
            return {"direction": "stable", "confidence": "low", "slope": 0}

        # Simple linear regression for trend
        n = len(issue_counts)
        x = list(range(n))
        y = issue_counts

        # Calculate slope
        x_mean = sum(x) / n
        y_mean = sum(y) / n

        numerator = sum((x[i] - x_mean) * (y[i] - y_mean) for i in range(n))
        denominator = sum((x[i] - x_mean) ** 2 for i in range(n))

        slope = numerator / denominator if denominator != 0 else 0

        # Determine trend direction and confidence
        if abs(slope) < 0.1:
            direction = "stable"
        elif slope > 0:
            direction = "increasing"
        else:
            direction = "decreasing"

        # Simple confidence based on R-squared
        y_pred = [x_mean + slope * (x[i] - x_mean) for i in range(n)]
        ss_res = sum((y[i] - y_pred[i]) ** 2 for i in range(n))
        ss_tot = sum((y[i] - y_mean) ** 2 for i in range(n))
        r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0

        if r_squared > 0.7:
            confidence = "high"
        elif r_squared > 0.4:
            confidence = "medium"
        else:
            confidence = "low"

        return {
            "direction": direction,
            "confidence": confidence,
            "slope": slope,
            "r_squared": r_squared,
        }

    def _analyze_peaks(self, aggregated_data: List[Dict]) -> Dict:
        """Analyze peaks and valleys in issue creation."""
        issue_counts = [data["total_issues"] for data in aggregated_data]

        if len(issue_counts) < 3:
            return {"peak_periods": [], "valley_periods": []}

        peaks = []
        valleys = []

        for i in range(1, len(issue_counts) - 1):
            if issue_counts[i] > issue_counts[i - 1] and issue_counts[i] > issue_counts[i + 1]:
                peaks.append({"period": aggregated_data[i]["period"], "issues": issue_counts[i]})
            elif issue_counts[i] < issue_counts[i - 1] and issue_counts[i] < issue_counts[i + 1]:
                valleys.append({"period": aggregated_data[i]["period"], "issues": issue_counts[i]})

        return {
            "peak_periods": peaks,
            "valley_periods": valleys,
        }

    def _analyze_distribution(self, issue_counts: List[int]) -> Dict:
        """Analyze the distribution of issue counts."""
        if not issue_counts:
            return {}

        import statistics

        mean = statistics.mean(issue_counts)
        median = statistics.median(issue_counts)

        try:
            stdev = statistics.stdev(issue_counts) if len(issue_counts) > 1 else 0
        except statistics.StatisticsError:
            stdev = 0

        return {
            "mean": mean,
            "median": median,
            "standard_deviation": stdev,
            "coefficient_of_variation": stdev / mean if mean > 0 else 0,
            "skewness": "normal" if abs(mean - median) < stdev * 0.5 else "skewed",
        }

    def _result_to_dict(self, result: IssueCreationResult) -> Dict:
        """Convert IssueCreationResult to dictionary."""
        return {
            "issue_key": result.issue_key,
            "summary": result.summary,
            "issue_type": result.issue_type,
            "priority": result.priority,
            "status": result.status,
            "assignee": result.assignee,
            "project_key": result.project_key,
            "labels": result.labels,
            "created_date": result.created_date,
            "created_date_parsed": result.created_date_parsed.isoformat(),
        }

    def _save_markdown_report(self, results: Dict, output_file: str):
        """Save results to Markdown file with formatted report."""
        query_info = results.get("query_info", {})
        summary = results.get("summary", {})

        with open(output_file, "w", encoding="utf-8") as f:
            f.write("# JIRA Issues Creation Analysis\n\n")
            f.write("## Query Information\n\n")
            f.write(f"- **Project**: {query_info.get('project_key', 'N/A')}\n")
            f.write(f"- **Period**: {query_info.get('start_date')} to {query_info.get('end_date')}\n")
            f.write(f"- **Aggregation**: {query_info.get('aggregation')}\n")
            f.write(f"- **Issue Types**: {', '.join(query_info.get('issue_types', []))}\n")
            if query_info.get("teams"):
                f.write(f"- **Teams**: {', '.join(query_info.get('teams', []))}\n")
            if query_info.get("labels"):
                f.write(f"- **Labels**: {', '.join(query_info.get('labels', []))}\n")

            f.write("\n## Summary\n\n")
            f.write(f"- **Total Issues**: {summary.get('total_issues', 0)}\n")
            f.write(f"- **Analysis Period**: {summary.get('analysis_period_days', 0)} days\n")
            f.write(f"- **Total Periods**: {summary.get('total_periods', 0)}\n")
            f.write(f"- **Periods with Issues**: {summary.get('periods_with_issues', 0)}\n")

            f.write("\n## Data by Period\n\n")
            f.write("| Period | Total Issues | Top Issue Type | Top Project |\n")
            f.write("|--------|--------------|----------------|-------------|\n")

            for data_point in results.get("data", []):
                period = data_point.get("period", "")
                total = data_point.get("total_issues", 0)
                issue_types = data_point.get("issue_types", {})
                projects = data_point.get("projects", {})

                top_type = max(issue_types.items(), key=lambda x: x[1])[0] if issue_types else "N/A"
                top_proj = max(projects.items(), key=lambda x: x[1])[0] if projects else "N/A"

                f.write(f"| {period} | {total} | {top_type} | {top_proj} |\n")

        self.logger.info(f"Markdown report saved to {output_file}")

    def _save_results(self, results: Dict, output_file: str):
        """Save results to file using the same pattern as resolution time service."""
        if output_file.endswith(".csv"):
            self._save_to_csv(results, output_file)
        else:
            # Default to JSON
            if not output_file.endswith(".json"):
                output_file += ".json"
            JSONManager.write_json(results, output_file)

        self.logger.info(f"Results saved to {output_file}")

    def _save_to_csv(self, results: Dict, output_file: str):
        """Save results to CSV file with comprehensive data."""
        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)

            # Write summary header
            writer.writerow(["ISSUES CREATION ANALYSIS RESULTS"])
            writer.writerow([])

            # Write query information
            query_info = results.get("query_info", {})
            writer.writerow(["QUERY INFORMATION"])
            writer.writerow(["Start Date", query_info.get("start_date", "")])
            writer.writerow(["End Date", query_info.get("end_date", "")])
            writer.writerow(["Aggregation", query_info.get("aggregation", "")])
            writer.writerow(["Issue Types", ", ".join(query_info.get("issue_types", []))])
            writer.writerow(["Projects", ", ".join(query_info.get("projects", []))])
            writer.writerow(["Labels", ", ".join(query_info.get("labels", []))])
            writer.writerow([])

            # Write summary statistics
            summary = results.get("summary", {})
            writer.writerow(["SUMMARY STATISTICS"])
            writer.writerow(["Total Issues", summary.get("total_issues", 0)])
            writer.writerow(["Analysis Period (Days)", summary.get("analysis_period_days", 0)])
            writer.writerow(["Total Periods", summary.get("total_periods", 0)])
            writer.writerow(["Periods with Issues", summary.get("periods_with_issues", 0)])
            writer.writerow([])

            # Write aggregated data
            writer.writerow(["AGGREGATED DATA BY PERIOD"])
            writer.writerow(
                [
                    "Period",
                    "Total Issues",
                    "Top Issue Type",
                    "Top Project",
                    "Top Priority",
                    "Top Assignee",
                ]
            )

            for data_point in results.get("data", []):
                # Get top items for each category
                issue_types = data_point.get("issue_types", {})
                projects = data_point.get("projects", {})
                priorities = data_point.get("priorities", {})
                assignees = data_point.get("assignees", {})

                top_issue_type = max(issue_types.items(), key=lambda x: x[1])[0] if issue_types else ""
                top_project = max(projects.items(), key=lambda x: x[1])[0] if projects else ""
                top_priority = max(priorities.items(), key=lambda x: x[1])[0] if priorities else ""
                top_assignee = max(assignees.items(), key=lambda x: x[1])[0] if assignees else ""

                writer.writerow(
                    [
                        data_point["period"],
                        data_point["total_issues"],
                        f"{top_issue_type} ({issue_types.get(top_issue_type, 0)})",
                        f"{top_project} ({projects.get(top_project, 0)})",
                        f"{top_priority} ({priorities.get(top_priority, 0)})",
                        f"{top_assignee} ({assignees.get(top_assignee, 0)})",
                    ]
                )

            # Add hierarchical summary section
            self._write_hierarchical_summary_csv(results, writer)

            # Write detailed summary if available
            if "detailed_summary" in results:
                writer.writerow([])
                writer.writerow(["DETAILED SUMMARY"])
                detailed_summary = results["detailed_summary"]

                writer.writerow(
                    [
                        "Average per Period",
                        detailed_summary.get("average_per_period", 0),
                    ]
                )
                writer.writerow(["Max per Period", detailed_summary.get("max_per_period", 0)])
                writer.writerow(["Min per Period", detailed_summary.get("min_per_period", 0)])
                writer.writerow(["Median per Period", detailed_summary.get("median_per_period", 0)])

                trend = detailed_summary.get("trend_analysis", {})
                writer.writerow(["Trend Direction", trend.get("direction", "stable")])
                writer.writerow(["Trend Confidence", trend.get("confidence", "low")])

            # Write individual issues
            writer.writerow([])
            writer.writerow(["INDIVIDUAL ISSUES"])
            writer.writerow(
                [
                    "Issue Key",
                    "Summary",
                    "Type",
                    "Priority",
                    "Status",
                    "Assignee",
                    "Project",
                    "Labels",
                    "Created Date",
                ]
            )

            for issue in results.get("issues", []):
                writer.writerow(
                    [
                        issue.get("issue_key", ""),
                        issue.get("summary", ""),
                        issue.get("issue_type", ""),
                        issue.get("priority", ""),
                        issue.get("status", ""),
                        issue.get("assignee", ""),
                        issue.get("project_key", ""),
                        ", ".join(issue.get("labels", [])),
                        issue.get("created_date", ""),
                    ]
                )

        self.logger.info(f"CSV results saved to {output_file}")

    def export_results(
        self,
        result: Dict,
        format: str = "json",
        output_file: Optional[str] = None,
    ) -> str:
        """
        Export results to file using OutputManager.

        Args:
            result: Analysis results
            format: Export format (json or csv)
            output_file: Output file path (auto-generated if not provided)

        Returns:
            Path to the exported file
        """
        if not output_file:
            aggregation = result["query_info"]["aggregation"]
            extension = f".{format}"
            output_file = OutputManager.get_output_path(
                sub_dir="issues-creation-analysis",
                file_name=f"analysis_{aggregation}",
                extension=extension,
            )

        try:
            self._save_results(result, output_file)

            # Also create a summary file if detailed summary exists
            if format == "csv" and "detailed_summary" in result:
                aggregation = result["query_info"]["aggregation"]
                summary_file = OutputManager.get_output_path(
                    sub_dir="issues-creation-analysis",
                    file_name=f"analysis_{aggregation}_summary",
                    extension=".csv",
                )
                self._save_summary_csv(result, summary_file)

            return output_file

        except Exception as e:
            self.logger.error(f"Error exporting results: {e}", exc_info=True)
            raise

    def _save_summary_csv(self, result: Dict, summary_file: str):
        """Save summary statistics to separate CSV file."""
        with open(summary_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)

            writer.writerow(["SUMMARY STATISTICS"])
            writer.writerow(["Metric", "Value"])

            summary = result.get("detailed_summary", {})
            for key, value in summary.items():
                if isinstance(value, dict):
                    writer.writerow([key.replace("_", " ").title(), ""])
                    for subkey, subvalue in value.items():
                        writer.writerow([f"  {subkey}", subvalue])
                else:
                    writer.writerow([key.replace("_", " ").title(), value])

        self.logger.info(f"Summary CSV saved to {summary_file}")

    def display_results(self, result: Dict, verbose: bool = False):
        """
        Display results in console format with hierarchical summaries.

        Args:
            result: Analysis results
            verbose: Enable verbose display with detailed information
        """
        print("\n" + "=" * 80)
        print("JIRA ISSUES CREATION ANALYSIS")
        print("=" * 80)

        # Query information
        query_info = result["query_info"]
        print(f"Period: {query_info['start_date']} to {query_info['end_date']}")
        print(f"Aggregation: {query_info['aggregation']}")
        print(f"Total Issues: {result['summary']['total_issues']}")

        # Filters
        if any(
            [
                query_info.get("issue_types"),
                query_info.get("projects"),
                query_info.get("labels"),
                query_info.get("additional_jql"),
            ]
        ):
            print("\nFilters Applied:")
            if query_info.get("issue_types"):
                print(f"  Issue Types: {', '.join(query_info['issue_types'])}")
            if query_info.get("projects"):
                print(f"  Projects: {', '.join(query_info['projects'])}")
            if query_info.get("labels"):
                print(f"  Labels: {', '.join(query_info['labels'])}")
            if query_info.get("additional_jql"):
                print(f"  Additional JQL: {query_info['additional_jql']}")

        # Main data breakdown
        aggregation_type = query_info["aggregation"]
        print(f"\n{aggregation_type.upper()} BREAKDOWN:")
        print("-" * 60)

        # Display detailed breakdown for each period
        for data_point in result["data"]:
            # Show issue count and top issue types
            total_issues = data_point["total_issues"]
            issue_types = data_point.get("issue_types", {})

            if total_issues > 0:
                # Get top issue type
                top_issue_type = max(issue_types.items(), key=lambda x: x[1]) if issue_types else None
                if top_issue_type:
                    print(f"{data_point['period']}: {total_issues} issues ({top_issue_type[0]}: {top_issue_type[1]})")
                else:
                    print(f"{data_point['period']}: {total_issues} issues")
            else:
                print(f"{data_point['period']}: 0 issues")

            # Only show detailed breakdown when verbose is enabled
            if verbose and data_point["issue_types"]:
                types_str = ", ".join([f"{t}: {c}" for t, c in data_point["issue_types"].items()])
                print(f"  Types: {types_str}")

                # Show additional details in verbose mode
                if data_point["projects"]:
                    projects_str = ", ".join([f"{p}: {c}" for p, c in data_point["projects"].items()])
                    print(f"  Projects: {projects_str}")

                if data_point["assignees"]:
                    assignees_str = ", ".join([f"{a}: {c}" for a, c in data_point["assignees"].items()])
                    print(f"  Assignees: {assignees_str}")

        # Generate and display hierarchical summary
        self._display_hierarchical_summary(result, verbose)

        # Detailed summary
        if "detailed_summary" in result:
            summary = result["detailed_summary"]
            print("\nOVERALL SUMMARY:")
            print("-" * 40)
            print(f"Average per period: {summary['average_per_period']:.1f}")
            print(f"Max per period: {summary['max_per_period']}")
            print(f"Min per period: {summary['min_per_period']}")

            trend = summary.get("trend_analysis", {})
            trend_dir = trend.get("direction", "stable")
            trend_conf = trend.get("confidence", "low")
            print(f"Trend: {trend_dir} ({trend_conf} confidence)")

            if summary.get("issue_type_distribution"):
                print("\nIssue Type Distribution:")
                for issue_type, count in summary["issue_type_distribution"].items():
                    print(f"  {issue_type}: {count}")

        print("\n" + "=" * 80)

    def _display_hierarchical_summary(self, result: Dict, verbose: bool = False):
        """
        Display hierarchical summary based on aggregation level.

        Args:
            result: Analysis results
            verbose: Enable verbose display
        """
        aggregation_type = result["query_info"]["aggregation"]
        data_points = result["data"]

        if not data_points:
            return

        print(f"\n{self._get_summary_level_name(aggregation_type)} SUMMARY:")
        print("-" * 50)

        # Generate summary based on aggregation type
        if aggregation_type == "daily":
            self._display_monthly_summary(data_points, verbose)
        elif aggregation_type == "weekly":
            self._display_monthly_summary(data_points, verbose)
        elif aggregation_type == "monthly":
            self._display_quarterly_summary(data_points, verbose)

    def _get_summary_level_name(self, aggregation_type: str) -> str:
        """Get the summary level name based on aggregation type."""
        if aggregation_type == "daily":
            return "MONTHLY"
        elif aggregation_type == "weekly":
            return "MONTHLY"
        elif aggregation_type == "monthly":
            return "QUARTERLY"
        return "PERIOD"

    def _display_monthly_summary(self, data_points: List[Dict], verbose: bool = False):
        """Display monthly summary for daily/weekly data."""
        from datetime import datetime
        from collections import defaultdict

        monthly_stats = defaultdict(
            lambda: {
                "total_issues": 0,
                "issue_types": defaultdict(int),
                "projects": defaultdict(int),
                "periods": [],
            }
        )

        # Aggregate by month
        for data_point in data_points:
            try:
                # Parse period to extract month
                period_str = data_point["period"]
                if "-" in period_str:
                    # Handle date formats like "2024-01-15"
                    period_date = datetime.strptime(period_str, "%Y-%m-%d")
                    month_key = period_date.strftime("%Y-%m")
                else:
                    # Handle other formats
                    month_key = period_str[:7] if len(period_str) >= 7 else period_str

                monthly_stats[month_key]["total_issues"] += data_point["total_issues"]
                monthly_stats[month_key]["periods"].append(period_str)

                # Aggregate issue types
                for issue_type, count in data_point.get("issue_types", {}).items():
                    monthly_stats[month_key]["issue_types"][issue_type] += count

                # Aggregate projects
                for project, count in data_point.get("projects", {}).items():
                    monthly_stats[month_key]["projects"][project] += count

            except ValueError:
                # Skip periods that can't be parsed
                continue

        # Display monthly summary
        for month, stats in sorted(monthly_stats.items()):
            total_issues = stats["total_issues"]
            if total_issues > 0:
                # Get top issue type and project
                top_issue_type = max(stats["issue_types"].items(), key=lambda x: x[1]) if stats["issue_types"] else None
                top_project = max(stats["projects"].items(), key=lambda x: x[1]) if stats["projects"] else None

                print(f"{month}: {total_issues} issues", end="")
                if top_issue_type:
                    print(f" ({top_issue_type[0]}: {top_issue_type[1]})", end="")
                if verbose and top_project:
                    print(f", {top_project[0]}: {top_project[1]}", end="")
                print()

                if verbose:
                    # Show issue type breakdown
                    types_str = ", ".join([f"{t}: {c}" for t, c in stats["issue_types"].items()])
                    print(f"  Types: {types_str}")

                    # Show period count
                    periods_with_issues = len(
                        [
                            p
                            for p in stats["periods"]
                            if any(dp["period"] == p and dp["total_issues"] > 0 for dp in data_points)
                        ]
                    )
                    print(f"  Periods: {len(stats['periods'])} ({periods_with_issues} with issues)")
            else:
                print(f"{month}: 0 issues")

    def _display_quarterly_summary(self, data_points: List[Dict], verbose: bool = False):
        """Display quarterly summary for monthly data."""
        from datetime import datetime
        from collections import defaultdict

        quarterly_stats = defaultdict(
            lambda: {
                "total_issues": 0,
                "issue_types": defaultdict(int),
                "projects": defaultdict(int),
                "months": [],
            }
        )

        # Aggregate by quarter
        for data_point in data_points:
            try:
                # Parse period to extract quarter
                period_str = data_point["period"]
                if "-" in period_str:
                    # Handle date formats like "2024-01"
                    period_date = datetime.strptime(period_str, "%Y-%m")
                    quarter = (period_date.month - 1) // 3 + 1
                    quarter_key = f"{period_date.year}-Q{quarter}"
                else:
                    # Skip periods that can't be parsed
                    continue

                quarterly_stats[quarter_key]["total_issues"] += data_point["total_issues"]
                quarterly_stats[quarter_key]["months"].append(period_str)

                # Aggregate issue types
                for issue_type, count in data_point.get("issue_types", {}).items():
                    quarterly_stats[quarter_key]["issue_types"][issue_type] += count

                # Aggregate projects
                for project, count in data_point.get("projects", {}).items():
                    quarterly_stats[quarter_key]["projects"][project] += count

            except ValueError:
                # Skip periods that can't be parsed
                continue

        # Display quarterly summary
        for quarter, stats in sorted(quarterly_stats.items()):
            total_issues = stats["total_issues"]
            if total_issues > 0:
                # Get top issue type and project
                top_issue_type = max(stats["issue_types"].items(), key=lambda x: x[1]) if stats["issue_types"] else None
                top_project = max(stats["projects"].items(), key=lambda x: x[1]) if stats["projects"] else None

                print(f"{quarter}: {total_issues} issues", end="")
                if top_issue_type:
                    print(f" ({top_issue_type[0]}: {top_issue_type[1]})", end="")
                if verbose and top_project:
                    print(f", {top_project[0]}: {top_project[1]}", end="")
                print()

                if verbose:
                    # Show issue type breakdown
                    types_str = ", ".join([f"{t}: {c}" for t, c in stats["issue_types"].items()])
                    print(f"  Types: {types_str}")

                    # Show month count
                    months_with_issues = len(
                        [
                            m
                            for m in stats["months"]
                            if any(dp["period"] == m and dp["total_issues"] > 0 for dp in data_points)
                        ]
                    )
                    print(f"  Months: {len(stats['months'])} ({months_with_issues} with issues)")
            else:
                print(f"{quarter}: 0 issues")

    def _write_hierarchical_summary_csv(self, results: Dict, writer):
        """Write hierarchical summary section to CSV."""
        aggregation_type = results["query_info"]["aggregation"]
        data_points = results["data"]

        if not data_points:
            return

        writer.writerow([])
        writer.writerow([f"{self._get_summary_level_name(aggregation_type)} SUMMARY"])

        # Generate and write summary based on aggregation type
        if aggregation_type == "daily":
            self._write_monthly_summary_csv(data_points, writer)
        elif aggregation_type == "weekly":
            self._write_monthly_summary_csv(data_points, writer)
        elif aggregation_type == "monthly":
            self._write_quarterly_summary_csv(data_points, writer)

    def _write_monthly_summary_csv(self, data_points: List[Dict], writer):
        """Write monthly summary to CSV for daily/weekly data."""
        from datetime import datetime
        from collections import defaultdict

        monthly_stats = defaultdict(
            lambda: {
                "total_issues": 0,
                "issue_types": defaultdict(int),
                "projects": defaultdict(int),
                "periods": [],
            }
        )

        # Aggregate by month
        for data_point in data_points:
            try:
                period_str = data_point["period"]
                if "-" in period_str:
                    period_date = datetime.strptime(period_str, "%Y-%m-%d")
                    month_key = period_date.strftime("%Y-%m")
                else:
                    month_key = period_str[:7] if len(period_str) >= 7 else period_str

                monthly_stats[month_key]["total_issues"] += data_point["total_issues"]
                monthly_stats[month_key]["periods"].append(period_str)

                for issue_type, count in data_point.get("issue_types", {}).items():
                    monthly_stats[month_key]["issue_types"][issue_type] += count

                for project, count in data_point.get("projects", {}).items():
                    monthly_stats[month_key]["projects"][project] += count

            except ValueError:
                continue

        # Write header
        writer.writerow(["Month", "Total Issues", "Top Issue Type", "Top Project", "Periods Count"])

        # Write monthly data
        for month, stats in sorted(monthly_stats.items()):
            total_issues = stats["total_issues"]
            top_issue_type = max(stats["issue_types"].items(), key=lambda x: x[1]) if stats["issue_types"] else ("", 0)
            top_project = max(stats["projects"].items(), key=lambda x: x[1]) if stats["projects"] else ("", 0)

            writer.writerow(
                [
                    month,
                    total_issues,
                    (f"{top_issue_type[0]} ({top_issue_type[1]})" if top_issue_type[0] else ""),
                    f"{top_project[0]} ({top_project[1]})" if top_project[0] else "",
                    len(stats["periods"]),
                ]
            )

    def _write_quarterly_summary_csv(self, data_points: List[Dict], writer):
        """Write quarterly summary to CSV for monthly data."""
        from datetime import datetime
        from collections import defaultdict

        quarterly_stats = defaultdict(
            lambda: {
                "total_issues": 0,
                "issue_types": defaultdict(int),
                "projects": defaultdict(int),
                "months": [],
            }
        )

        # Aggregate by quarter
        for data_point in data_points:
            try:
                period_str = data_point["period"]
                if "-" in period_str:
                    period_date = datetime.strptime(period_str, "%Y-%m")
                    quarter = (period_date.month - 1) // 3 + 1
                    quarter_key = f"{period_date.year}-Q{quarter}"
                else:
                    continue

                quarterly_stats[quarter_key]["total_issues"] += data_point["total_issues"]
                quarterly_stats[quarter_key]["months"].append(period_str)

                for issue_type, count in data_point.get("issue_types", {}).items():
                    quarterly_stats[quarter_key]["issue_types"][issue_type] += count

                for project, count in data_point.get("projects", {}).items():
                    quarterly_stats[quarter_key]["projects"][project] += count

            except ValueError:
                continue

        # Write header
        writer.writerow(["Quarter", "Total Issues", "Top Issue Type", "Top Project", "Months Count"])

        # Write quarterly data
        for quarter, stats in sorted(quarterly_stats.items()):
            total_issues = stats["total_issues"]
            top_issue_type = max(stats["issue_types"].items(), key=lambda x: x[1]) if stats["issue_types"] else ("", 0)
            top_project = max(stats["projects"].items(), key=lambda x: x[1]) if stats["projects"] else ("", 0)

            writer.writerow(
                [
                    quarter,
                    total_issues,
                    (f"{top_issue_type[0]} ({top_issue_type[1]})" if top_issue_type[0] else ""),
                    f"{top_project[0]} ({top_project[1]})" if top_project[0] else "",
                    len(stats["months"]),
                ]
            )
