"""
JIRA Issue Resolution Time Service

This service provides functionality to analyze issue resolution time by fetching
resolved issues and calculating statistics on how long it takes to move from
"start" to "done" status, grouped by issue type and priority.
"""

import csv
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from utils.data.json_manager import JSONManager
from utils.jira.jira_assistant import JiraAssistant
from utils.logging.logging_manager import LogManager
from utils.output_manager import OutputManager
from domains.syngenta.jira.issue_adherence_service import TimePeriodParser
import numpy as np


@dataclass
class IssueResolutionResult:
    """Data class to hold issue resolution analysis results."""

    issue_key: str
    summary: str
    issue_type: str
    priority: str
    status: str
    assignee: Optional[str]
    squad: Optional[str]
    fixversion: Optional[str]
    created_date: Optional[str]
    first_in_progress_date: Optional[str]
    resolved_date: Optional[str]
    resolution_time_days: Optional[float]


class IssueResolutionTimeService:
    """Service class for issue resolution time analysis."""

    def __init__(self):
        self.jira_assistant = JiraAssistant()
        self.logger = LogManager.get_instance().get_logger("IssueResolutionTimeService")
        self.time_parser = TimePeriodParser()

    def analyze_resolution_time(
        self,
        project_key: str,
        time_period: str,
        issue_types: List[str],
        squad: Optional[str] = None,
        fixversion: Optional[str] = None,
        assignee: Optional[str] = None,
        verbose: bool = False,
        output_file: Optional[str] = None,
        exclude_outliers: bool = False,
        outlier_strategy: Optional[str] = None,
        include_raw: bool = False,
    ) -> Dict:
        """
        Analyze issue resolution time for issues resolved within a specified time period.

        Args:
            project_key (str): JIRA project key
            time_period (str): Time period to analyze
            issue_types (List[str]): List of issue types to include
            squad (Optional[str]): Squad name to filter by
            fixversion (Optional[str]): Fix version to filter by
            assignee (Optional[str]): Assignee to filter by
            verbose (bool): Enable verbose output
            output_file (Optional[str]): Output file path
            exclude_outliers (bool): Whether to exclude outliers from calculations
            outlier_strategy (str): Strategy for outlier detection: 'none', 'trim', 'iqr', 'zscore'

        Returns:
            Dict: Analysis results
        """
        self.logger.info(
            f"Starting resolution time analysis for project {project_key}, "
            f"time period: {time_period}, issue types: {issue_types}"
        )

        try:
            # Parse time period
            start_date, end_date = self.time_parser.parse_time_period(time_period)
            self.logger.info(f"Analysis period: {start_date} to {end_date}")

            # Set default outlier strategy to IQR if excluding outliers and not specified
            if exclude_outliers and not outlier_strategy:
                outlier_strategy = "iqr"
            if outlier_strategy is None:
                outlier_strategy = "iqr"

            # Build JQL query
            jql_query = self._build_jql_query(
                project_key,
                start_date,
                end_date,
                issue_types,
                squad,
                fixversion,
                assignee,
            )

            # Fetch issues with pagination to get all results
            self.logger.info(f"Executing JQL query: {jql_query}")
            # Fetch issues - JIRA limits maxResults to 100 when requesting custom fields
            # Our fetch_issues method handles pagination automatically to get all results
            issues = self.jira_assistant.fetch_issues(
                jql_query=jql_query,
                fields=(
                    "key,summary,issuetype,priority,status,assignee,customfield_10851,"
                    "customfield_10015,created,resolutiondate"
                ),
                max_results=100,  # Use 100 to work with JIRA limitation on custom fields
                expand_changelog=True,
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

            # Analyze each issue and collect debugging information
            resolution_results = []
            all_status_names = set()
            issues_with_empty_changelog = 0

            for issue in issues:
                # Collect all status names for debugging
                changelog = issue.get("changelog", {}).get("histories", [])
                if not changelog:
                    issues_with_empty_changelog += 1
                else:
                    for history in changelog:
                        for item in history.get("items", []):
                            if item.get("field") == "status":
                                from_status = item.get("fromString", "")
                                to_status = item.get("toString", "")
                                if from_status:
                                    all_status_names.add(from_status)
                                if to_status:
                                    all_status_names.add(to_status)

                result = self._analyze_issue_resolution(issue)
                if result and result.resolution_time_days is not None:
                    resolution_results.append(result)

            # Log debugging information
            self.logger.info(f"Analyzed {len(resolution_results)} issues with resolution time data")
            self.logger.info(
                f"Issues with empty changelog: {issues_with_empty_changelog}/{len(issues)}"
            )
            if all_status_names:
                sorted_statuses = sorted(all_status_names)
                self.logger.info(f"All unique status names found: {sorted_statuses}")
            else:
                self.logger.warning(
                    "No status names found in any changelog - this may indicate a data issue"
                )

            # Calculate statistics
            stats_by_type_priority = self._calculate_statistics_by_type_priority(
                resolution_results, exclude_outliers, outlier_strategy
            )
            overall_stats = self._calculate_overall_statistics(
                resolution_results, exclude_outliers, outlier_strategy
            )

            # Calculate outlier summary if exclude_outliers is enabled
            outlier_summary = {}
            if exclude_outliers:
                outlier_summary = self._calculate_outlier_summary(
                    resolution_results, outlier_strategy
                )

            # Build result with clean output structure
            result = {
                "query_info": {
                    "project_key": project_key,
                    "time_period": time_period,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "issue_types": issue_types,
                    "squad": squad,
                    "fixversion": fixversion,
                    "assignee": assignee,
                    "exclude_outliers": exclude_outliers,
                    "outlier_strategy": outlier_strategy,
                },
                "summary": {
                    "total_issues": len(resolution_results),
                },
                "by_type_and_priority": self._clean_stats_output(
                    stats_by_type_priority, include_raw
                ),
                "overall_stats": self._clean_stats_output({"overall": overall_stats}, include_raw)[
                    "overall"
                ],
                # Add all issues for charting and compliance calculation
                "issues": [self._result_to_dict(r) for r in resolution_results],
            }

            # Add debugging/verbose information if requested
            if include_raw or verbose:
                result["debug_info"] = {
                    "jql_query": jql_query,
                    "issues": [self._result_to_dict(r) for r in resolution_results],
                }

                # Add outlier summary if outlier exclusion is enabled
                if exclude_outliers:
                    result["debug_info"]["outlier_summary"] = outlier_summary
            else:
                # Only add outlier summary to main result if not in debug mode
                if exclude_outliers:
                    result["outlier_summary"] = outlier_summary

            # Save results if output file specified
            if output_file:
                self._save_results(result, output_file)
            else:
                # Generate default output file in organized structure
                output_path = OutputManager.get_output_path(
                    "issue-resolution-time", f"resolution_time_{project_key}"
                )
                self._save_results(result, output_path)

            # Print verbose results if requested
            if verbose:
                self._print_verbose_results(resolution_results, stats_by_type_priority)

            return result

        except Exception as e:
            self.logger.error(f"Error analyzing resolution time: {e}", exc_info=True)
            raise

    def _build_jql_query(
        self,
        project_key: str,
        start_date: datetime,
        end_date: datetime,
        issue_types: List[str],
        squad: Optional[str],
        fixversion: Optional[str],
        assignee: Optional[str],
    ) -> str:
        """Build JQL query for fetching issues."""

        # Base query - issues resolved in the time period
        jql_parts = [f"project = '{project_key}'"]

        # Issue types
        if issue_types:
            types_str = "', '".join(issue_types)
            jql_parts.append(f"type in ('{types_str}')")

        # Time period - using resolution date to capture issues resolved in the period
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        jql_parts.append(f"resolved >= '{start_date_str}' AND resolved <= '{end_date_str}'")

        # Squad filter (using custom field for squad)
        if squad:
            jql_parts.append(f"'Squad[Dropdown]' = '{squad}'")

        # Fix version filter
        if fixversion:
            jql_parts.append(f"fixVersion = '{fixversion}'")

        # Assignee filter
        if assignee:
            jql_parts.append(f"assignee = '{assignee}'")

        # Exclude archived issues and ensure we get resolved issues
        # We'll filter DONE vs ARCHIVED in the analysis logic instead of here
        jql_parts.append("status != '11 ARCHIVED'")
        jql_parts.append("statusCategory = Done")

        return " AND ".join(jql_parts)

    def _analyze_issue_resolution(self, issue: Dict) -> Optional[IssueResolutionResult]:
        """Analyze resolution time for a single issue."""

        try:
            fields = issue.get("fields", {})

            # Extract basic issue information
            issue_key = issue.get("key", "")
            summary = fields.get("summary", "")
            issue_type = fields.get("issuetype", {}).get("name", "")
            priority = fields.get("priority", {}).get("name", "Unknown")
            status = fields.get("status", {}).get("name", "")

            # Check if issue is archived - exclude from calculations
            if self._is_issue_archived(issue):
                self.logger.debug(f"Issue {issue_key} is archived - excluding from calculations")
                return None

            # Extract assignee
            assignee_field = fields.get("assignee")
            assignee = assignee_field.get("emailAddress") if assignee_field else None

            # Extract squad (custom field)
            squad_field = fields.get("customfield_10851")
            squad = squad_field.get("value") if squad_field else None

            # Extract fix version (custom field)
            fixversion_field = fields.get("customfield_10015")
            fixversion = (
                fixversion_field[0] if fixversion_field and len(fixversion_field) > 0 else None
            )

            # Extract dates
            created_date = fields.get("created")

            # Find the first DONE date from changelog, with fallback to resolutiondate
            first_done_date = self._find_first_done_date(issue)

            # Fallback to resolutiondate if changelog parsing fails
            if not first_done_date:
                resolution_date = fields.get("resolutiondate")
                if resolution_date:
                    first_done_date = resolution_date
                    self.logger.debug(f"Issue {issue_key}: Using resolutiondate as fallback")
                else:
                    self.logger.debug(
                        f"Issue {issue_key} has no DONE transition date or resolutiondate"
                    )
                    return None

            # Find first "In Progress" or equivalent status from changelog
            first_in_progress_date = self._find_first_in_progress_date(issue)

            # Calculate resolution time based on issue type
            resolution_time_days = None
            start_to_resolve_days = None  # For debugging/internal use
            created_to_resolve_days = None  # For debugging/internal use

            # Calculate both metrics for debugging purposes
            if first_in_progress_date and first_done_date:
                start_dt = datetime.fromisoformat(first_in_progress_date.replace("Z", "+00:00"))
                end_dt = datetime.fromisoformat(first_done_date.replace("Z", "+00:00"))
                start_to_resolve_days = (end_dt - start_dt).total_seconds() / (24 * 3600)

            if created_date and first_done_date:
                start_dt = datetime.fromisoformat(created_date.replace("Z", "+00:00"))
                end_dt = datetime.fromisoformat(first_done_date.replace("Z", "+00:00"))
                created_to_resolve_days = (end_dt - start_dt).total_seconds() / (24 * 3600)

            # Use the correct metric based on issue type
            if issue_type in ["Bug", "Support"]:
                # For Bug and Support, use created_date to resolved_date
                resolution_time_days = created_to_resolve_days
                if resolution_time_days is None:
                    self.logger.debug(f"Issue {issue_key}: No created date or resolved date found")
                    return None
            else:
                # For other types, use start_date (first_in_progress_date) to resolved_date
                resolution_time_days = start_to_resolve_days
                if resolution_time_days is None:
                    # Fallback to created date if no in-progress date found
                    resolution_time_days = created_to_resolve_days
                    first_in_progress_date = created_date
                    if resolution_time_days is None:
                        self.logger.debug(
                            f"Issue {issue_key}: No valid start date or resolved date found"
                        )
                        return None

            return IssueResolutionResult(
                issue_key=issue_key,
                summary=summary,
                issue_type=issue_type,
                priority=priority,
                status=status,
                assignee=assignee,
                squad=squad,
                fixversion=fixversion,
                created_date=created_date,
                first_in_progress_date=first_in_progress_date,
                resolved_date=first_done_date,  # Use first DONE date instead of resolution date
                resolution_time_days=resolution_time_days,
            )

        except Exception as e:
            self.logger.warning(f"Error analyzing issue {issue.get('key', 'unknown')}: {e}")
            return None

    def _find_first_in_progress_date(self, issue: Dict) -> Optional[str]:
        """Find the first time an issue was moved to 'In Progress' or similar status."""

        changelog = issue.get("changelog", {}).get("histories", [])
        issue_key = issue.get("key", "unknown")

        # Status names that indicate work has started (based on CWS workflows)
        start_statuses = [
            "07 STARTED",
        ]

        # Debug: Log all status transitions for this issue
        status_transitions = []
        for history in changelog:
            for item in history.get("items", []):
                if item.get("field") == "status":
                    status_transitions.append(
                        {
                            "date": history.get("created"),
                            "from": item.get("fromString"),
                            "to": item.get("toString"),
                        }
                    )

        if status_transitions:
            self.logger.debug(f"Issue {issue_key} status transitions: {status_transitions}")

        # Look for the first transition to any "in progress" status
        for history in changelog:
            for item in history.get("items", []):
                if item.get("field") == "status":
                    to_status = item.get("toString", "")

                    # Check if transitioning TO a start status
                    if to_status in start_statuses:
                        self.logger.debug(
                            f"Issue {issue_key}: Found first in-progress transition to "
                            f"'{to_status}' on {history.get('created')}"
                        )
                        return history.get("created")

                    # Also check for common patterns in status names
                    keywords = ["started"]  # Focus on "started" pattern primarily
                    if any(keyword in to_status.lower() for keyword in keywords):
                        self.logger.debug(
                            f"Issue {issue_key}: Found pattern-matched in-progress status "
                            f"'{to_status}' on {history.get('created')}"
                        )
                        return history.get("created")

        # If no in-progress status found, log this for debugging
        self.logger.debug(
            f"Issue {issue_key}: No in-progress status found in "
            f"{len(status_transitions)} transitions"
        )
        return None

    def _is_issue_archived(self, issue: Dict) -> bool:
        """
        Check if an issue is archived (current status is '11 ARCHIVED' or similar).

        Args:
            issue (Dict): JIRA issue data

        Returns:
            bool: True if issue is archived, False otherwise
        """
        current_status = issue.get("fields", {}).get("status", {}).get("name", "")

        # Check for archived status patterns
        archived_statuses = ["11 ARCHIVED", "ARCHIVED", "Archived"]

        return current_status in archived_statuses or "archived" in current_status.lower()

    def _find_first_done_date(self, issue: Dict) -> Optional[str]:
        """
        Find the first time an issue was moved to 'DONE' status.
        Once an issue reaches DONE, that's the final resolution time regardless of later moves.

        Args:
            issue (Dict): JIRA issue data

        Returns:
            Optional[str]: First DONE transition date or None if never reached DONE
        """
        changelog = issue.get("changelog", {}).get("histories", [])
        issue_key = issue.get("key", "unknown")

        # DONE status names that indicate completion
        done_statuses = ["DONE", "10 DONE"]

        # Debug: Log all status transitions for this issue
        status_transitions = []
        for history in changelog:
            for item in history.get("items", []):
                if item.get("field") == "status":
                    status_transitions.append(
                        {
                            "date": history.get("created"),
                            "from": item.get("fromString"),
                            "to": item.get("toString"),
                        }
                    )

        if status_transitions:
            self.logger.debug(f"Issue {issue_key} status transitions: {status_transitions}")

        # Look for the first transition TO any DONE status
        for history in changelog:
            for item in history.get("items", []):
                if item.get("field") == "status":
                    to_status = item.get("toString", "")

                    # Check if transitioning TO a done status
                    if to_status in done_statuses:
                        self.logger.debug(
                            f"Issue {issue_key}: Found first DONE transition to "
                            f"'{to_status}' on {history.get('created')}"
                        )
                        return history.get("created")

                    # Also check for common patterns in status names
                    done_keywords = ["done", "closed", "resolved"]
                    if any(keyword in to_status.lower() for keyword in done_keywords):
                        self.logger.debug(
                            f"Issue {issue_key}: Found pattern-matched DONE status "
                            f"'{to_status}' on {history.get('created')}"
                        )
                        return history.get("created")

        # If no DONE status found, log this for debugging
        self.logger.debug(
            f"Issue {issue_key}: No DONE status found in " f"{len(status_transitions)} transitions"
        )
        return None

    def _calculate_statistics_by_type_priority(
        self,
        results: List[IssueResolutionResult],
        exclude_outliers: bool = False,
        outlier_strategy: str = "iqr",
    ) -> Dict:
        """Calculate enhanced statistics grouped by issue type and priority.
        Adds SLA priority consistency and diagnostics.
        """
        stats = {}
        # Group by type and priority
        for result in results:
            issue_type = result.issue_type
            priority = result.priority
            if issue_type not in stats:
                stats[issue_type] = {}
            if priority not in stats[issue_type]:
                stats[issue_type][priority] = []
            if result.resolution_time_days is not None:
                stats[issue_type][priority].append(result.resolution_time_days)
        # Calculate statistics for each group
        for issue_type in stats:
            for priority in stats[issue_type]:
                times = stats[issue_type][priority]
                if times:
                    enhanced = self._calculate_enhanced_stats(
                        times, exclude_outliers, outlier_strategy
                    )
                    # Add diagnostic p95_trimmed_days for all outlier strategies
                    diag = {}
                    for strat in ["trim", "iqr", "zscore"]:
                        filtered, _, _ = self._filter_outliers(times, strat)
                        if filtered and len(filtered) != len(times):
                            diag[strat] = float(np.percentile(filtered, 95))
                        else:
                            diag[strat] = float(np.percentile(times, 95))
                    enhanced["diagnostic_p95_trimmed_days"] = diag
                    stats[issue_type][priority] = enhanced
            # Enforce SLA priority consistency for this issue_type
            priority_order = ["Critical [P1]", "High [P2]", "Medium [P3]", "Low [P4]"]
            sla_by_priority = {}
            for p in priority_order:
                if p in stats[issue_type]:
                    sla_by_priority[p] = stats[issue_type][p]["suggested_sla_days"]
            for i in range(1, len(priority_order)):
                prev = priority_order[i - 1]
                curr = priority_order[i]
                if prev in sla_by_priority and curr in sla_by_priority:
                    if sla_by_priority[curr] < sla_by_priority[prev]:
                        sla_by_priority[curr] = sla_by_priority[prev]
                        stats[issue_type][curr]["suggested_sla_days"] = sla_by_priority[prev]
            # Add sla_risk_level based on p95_ci width
            for p in priority_order:
                if p in stats[issue_type]:
                    ci_lower = stats[issue_type][p].get("p95_ci_lower")
                    ci_upper = stats[issue_type][p].get("p95_ci_upper")
                    if ci_lower is not None and ci_upper is not None:
                        width = ci_upper - ci_lower
                        if width <= 2:
                            risk = "low"
                        elif width <= 5:
                            risk = "medium"
                        else:
                            risk = "high"
                        stats[issue_type][p]["sla_risk_level"] = risk
                    else:
                        stats[issue_type][p]["sla_risk_level"] = None
        return stats

    def _calculate_enhanced_stats(
        self,
        times: List[float],
        exclude_outliers: bool = False,
        outlier_strategy: str = "iqr",
    ) -> Dict:
        """
        Calculate enhanced statistics for a list of resolution times with flexible outlier handling.
        Implements:
        - SLA suggestion as math.ceil(p90) (industry best practice for 90% compliance)
        - SLA compliance percentage (actual % of issues resolved within suggested SLA)
        - IQR as default outlier strategy for robust filtering
        - 95% confidence interval for P90 if count < 30 (conservative for small samples)
        """
        if not times:
            return {}

        original_count = len(times)
        filtered_times = times
        outliers_removed = 0
        outlier_threshold = None

        # Apply outlier filtering if requested and enough data
        if exclude_outliers and outlier_strategy != "none" and original_count > 10:
            filtered_times, outliers_removed, outlier_threshold = self._filter_outliers(
                times, outlier_strategy
            )
            self.logger.debug(
                f"Outlier filtering: original_count={original_count}, "
                f"filtered_count={len(filtered_times)}, outliers_removed={outliers_removed}, "
                f"threshold={outlier_threshold}"
            )
            self.logger.debug(
                f"Outlier filtering applied: {outliers_removed} outliers removed "
                f"using {outlier_strategy} strategy, threshold: {outlier_threshold}"
            )

        # Use filtered data for calculations if outliers were excluded
        calculation_data = filtered_times if exclude_outliers and outliers_removed > 0 else times
        count = len(calculation_data)

        # Ensure we have data to calculate with
        if count == 0:
            self.logger.warning("No data left after outlier filtering - using original data")
            calculation_data = times
            count = len(calculation_data)

        # Debug logging for calculation data choice
        self.logger.debug(
            f"Calculation data: original={len(times)}, filtered={len(filtered_times)}, "
            f"using={'filtered' if exclude_outliers and outliers_removed > 0 else 'original'}, "
            f"final_count={count}"
        )

        # Calculate main statistics using the appropriate dataset
        if count > 0:
            median_days = float(np.median(calculation_data))
            p80_days = float(np.percentile(calculation_data, 80))
            p90_days = float(np.percentile(calculation_data, 90))
            p95_days = float(np.percentile(calculation_data, 95))
            std_dev = float(np.std(calculation_data, ddof=1)) if count > 1 else 0.0
            max_days = float(max(calculation_data))
            mean_days = float(np.mean(calculation_data))
        else:
            median_days = p80_days = p90_days = p95_days = std_dev = max_days = mean_days = 0.0

        # Calculate p95_trimmed_days for diagnostics
        p95_trimmed_days = p95_days
        if filtered_times and len(filtered_times) != len(times):
            p95_trimmed_days = float(np.percentile(filtered_times, 95))

        # --- SLA suggestion: use math.ceil(p90) for 90% compliance (industry best practice) ---
        suggested_sla_days = math.ceil(p90_days)

        # --- Confidence interval for P90 if count < 30 (conservative for small samples) ---
        p90_ci_lower = None
        p90_ci_upper = None
        if count < 30 and count > 1:
            p90_ci_lower, p90_ci_upper = self._calculate_p90_confidence_interval(calculation_data)
            # For small samples, use upper bound for suggested SLA
            suggested_sla_days = math.ceil(p90_ci_upper)

        # --- SLA compliance: % of issues resolved within suggested SLA ---
        # Calculate compliance AFTER potentially adjusting SLA for confidence interval
        sla_compliance_count = sum(1 for t in calculation_data if t <= suggested_sla_days)
        sla_compliance_percentage = (sla_compliance_count / count * 100) if count > 0 else 0.0
        # Clamp to [0, 100]
        sla_compliance_percentage = min(100.0, max(0.0, sla_compliance_percentage))

        # Debug logging for compliance calculation
        self.logger.debug(f"suggested_sla_days: {suggested_sla_days}")
        self.logger.debug(f"calculation_data (first 20): {calculation_data[:20]}")
        self.logger.debug(f"sla_compliance_count: {sla_compliance_count} / {count}")
        self.logger.debug(f"sla_compliance_percentage: {sla_compliance_percentage}")

        # --- Validations ---
        validation_warnings = []
        if suggested_sla_days < p90_days:
            validation_warnings.append("Suggested SLA is less than P90")
        if p95_trimmed_days > p95_days:
            validation_warnings.append("P95 trimmed is greater than regular P95")
        if not (0.0 <= sla_compliance_percentage <= 100.0):
            validation_warnings.append("SLA compliance percentage out of bounds")

        buckets = self._calculate_resolution_buckets(calculation_data)

        stats = {
            "count": original_count,  # Always show original count
            "effective_count": count,  # Show count after outlier removal
            "median_days": median_days,
            "p80_days": p80_days,
            "p90_days": p90_days,
            "p95_days": p95_days,
            "p95_trimmed_days": p95_trimmed_days,
            "std_dev": std_dev,
            "max_days": max_days,
            "suggested_sla_days": suggested_sla_days,
            "sla_compliance_percentage": sla_compliance_percentage,
            "outlier_strategy": outlier_strategy,
            "outliers_removed": outliers_removed,
            "resolution_buckets": buckets,
            # Keep legacy fields for backwards compatibility
            "median_resolution_time": median_days,
            "p95_resolution_time": p95_days,
            "average_resolution_time": mean_days,
            "min_resolution_time": float(min(calculation_data)) if count > 0 else 0.0,
            "max_resolution_time": max_days,
        }
        if p90_ci_lower is not None:
            stats["p90_ci_lower"] = p90_ci_lower
            stats["p90_ci_upper"] = p90_ci_upper
        if outlier_threshold is not None:
            stats["outlier_threshold"] = outlier_threshold
        if validation_warnings:
            stats["validation_warnings"] = validation_warnings
        return stats

    def _calculate_overall_statistics(
        self,
        results: List[IssueResolutionResult],
        exclude_outliers: bool = False,
        outlier_strategy: str = "trim",
    ) -> Dict:
        """Calculate overall statistics across all issues."""

        times = [r.resolution_time_days for r in results if r.resolution_time_days is not None]

        if not times:
            return {
                "total_issues": 0,
                "median_days": 0,
                "p80_days": 0,
                "p90_days": 0,
                "p95_days": 0,
                "p95_trimmed_days": 0,
                "std_dev": 0,
                "max_days": 0,
                "suggested_sla_days": 0,
                # Legacy fields
                "median_resolution_time": 0,
                "p95_resolution_time": 0,
                "average_resolution_time": 0,
                "min_resolution_time": 0,
                "max_resolution_time": 0,
            }

        enhanced_stats = self._calculate_enhanced_stats(times, exclude_outliers, outlier_strategy)
        enhanced_stats["total_issues"] = enhanced_stats["count"]  # Use original count
        return enhanced_stats

    def _calculate_outlier_summary(
        self, results: List[IssueResolutionResult], outlier_strategy: str = "trim"
    ) -> Dict:
        """Calculate summary of outliers excluded per issue type."""

        outlier_summary = {}

        # Group by issue type
        by_type = {}
        for result in results:
            if result.resolution_time_days is not None:
                issue_type = result.issue_type
                if issue_type not in by_type:
                    by_type[issue_type] = []
                by_type[issue_type].append(result.resolution_time_days)

        # Calculate outliers for each type using the specified strategy
        for issue_type, times in by_type.items():
            if len(times) > 10:  # Only calculate for reasonable sample sizes
                _, outliers_removed, threshold = self._filter_outliers(times, outlier_strategy)
                total_count = len(times)

                outlier_summary[issue_type] = {
                    "total_issues": total_count,
                    "outliers_excluded": outliers_removed,
                    "outlier_percentage": (outliers_removed / total_count) * 100,
                    "outlier_threshold_days": threshold or 0,
                    "outlier_strategy": outlier_strategy,
                }
            else:
                outlier_summary[issue_type] = {
                    "total_issues": len(times),
                    "outliers_excluded": 0,
                    "outlier_percentage": 0,
                    "outlier_threshold_days": 0,
                    "outlier_strategy": outlier_strategy,
                }

        return outlier_summary

    def _result_to_dict(self, result: IssueResolutionResult) -> Dict:
        """Convert result to dictionary."""
        return {
            "issue_key": result.issue_key,
            "summary": result.summary,
            "issue_type": result.issue_type,
            "priority": result.priority,
            "status": result.status,
            "assignee": result.assignee,
            "squad": result.squad,
            "fixversion": result.fixversion,
            "created_date": result.created_date,
            "first_in_progress_date": result.first_in_progress_date,
            "resolved_date": result.resolved_date,
            "resolution_time_days": result.resolution_time_days,
        }

    def _save_results(self, results: Dict, output_file: str):
        """Save results to file."""

        if output_file.endswith(".csv"):
            self._save_to_csv(results, output_file)
        else:
            # Default to JSON
            if not output_file.endswith(".json"):
                output_file += ".json"
            JSONManager.write_json(results, output_file)

        self.logger.info(f"Results saved to {output_file}")

    def _save_to_csv(self, results: Dict, output_file: str):
        """Save results to CSV file with enhanced statistics."""

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)

            # Write summary statistics first
            writer.writerow(["RESOLUTION TIME STATISTICS BY TYPE AND PRIORITY"])
            writer.writerow([])

            # Header for statistics
            writer.writerow(
                [
                    "Issue Type",
                    "Priority",
                    "Count",
                    "Median Days",
                    "P80 Days",
                    "P90 Days",
                    "P95 Days",
                    "P95 Trimmed Days",
                    "Std Dev",
                    "Max Days",
                    "Suggested SLA Days",
                    "Outlier Strategy",
                    "Outliers Removed",
                    "Resolution Buckets (<1d)",
                    "Resolution Buckets (1-3d)",
                    "Resolution Buckets (3-7d)",
                    "Resolution Buckets (7-14d)",
                    "Resolution Buckets (14-30d)",
                    "Resolution Buckets (>30d)",
                ]
            )

            # Write statistics data
            stats = results.get("by_type_and_priority", {})
            for issue_type, priorities in stats.items():
                for priority, data in priorities.items():
                    buckets = data.get("resolution_buckets", {})
                    writer.writerow(
                        [
                            issue_type,
                            priority,
                            data.get("count", 0),
                            round(data.get("median_days", 0), 2),
                            round(data.get("p80_days", 0), 2),
                            round(data.get("p90_days", 0), 2),
                            round(data.get("p95_days", 0), 2),
                            round(data.get("p95_trimmed_days", 0), 2),
                            round(data.get("std_dev", 0), 2),
                            round(data.get("max_days", 0), 2),
                            data.get("suggested_sla_days", 0),
                            data.get("outlier_strategy", "none"),
                            data.get("outliers_removed", 0),
                            buckets.get("<1d", 0),
                            buckets.get("1-3d", 0),
                            buckets.get("3-7d", 0),
                            buckets.get("7-14d", 0),
                            buckets.get("14-30d", 0),
                            buckets.get(">30d", 0),
                        ]
                    )

            # Add outlier summary if available
            if "outlier_summary" in results:
                writer.writerow([])
                writer.writerow(["OUTLIER SUMMARY"])
                writer.writerow(
                    [
                        "Issue Type",
                        "Total Issues",
                        "Outliers Excluded",
                        "Outlier Percentage",
                        "Outlier Threshold Days",
                        "Outlier Strategy",
                    ]
                )

                for issue_type, summary in results["outlier_summary"].items():
                    writer.writerow(
                        [
                            issue_type,
                            summary.get("total_issues", 0),
                            summary.get("outliers_excluded", 0),
                            round(summary.get("outlier_percentage", 0), 2),
                            round(summary.get("outlier_threshold_days", 0), 2),
                            summary.get("outlier_strategy", "none"),
                        ]
                    )

            # Separator and detailed issue data
            writer.writerow([])
            writer.writerow(["DETAILED ISSUE DATA"])
            writer.writerow([])

            # Write header for issue details
            writer.writerow(
                [
                    "Issue Key",
                    "Summary",
                    "Type",
                    "Priority",
                    "Status",
                    "Assignee",
                    "Squad",
                    "Fix Version",
                    "Created Date",
                    "First In Progress Date",
                    "Resolved Date",
                    "Resolution Time (Days)",
                ]
            )

            # Write issue data
            for issue in results.get("issues", []):
                resolution_time = issue.get("resolution_time_days", 0)
                formatted_time = round(resolution_time, 2) if resolution_time else ""
                writer.writerow(
                    [
                        issue.get("issue_key", ""),
                        issue.get("summary", ""),
                        issue.get("issue_type", ""),
                        issue.get("priority", ""),
                        issue.get("status", ""),
                        issue.get("assignee", ""),
                        issue.get("squad", ""),
                        issue.get("fixversion", ""),
                        issue.get("created_date", ""),
                        issue.get("first_in_progress_date", ""),
                        issue.get("resolved_date", ""),
                        formatted_time,
                    ]
                )

        self.logger.info(f"Enhanced CSV results saved to {output_file}")

    def _print_verbose_results(
        self, results: List[IssueResolutionResult], _stats_by_type_priority: Dict
    ):
        """Print verbose results to console."""

        print("\n" + "=" * 80)
        print("DETAILED ISSUE RESOLUTION TIME ANALYSIS")
        print("=" * 80)

        # Group by type and priority for display
        type_priority_groups = {}
        for result in results:
            key = f"{result.issue_type} - {result.priority}"
            if key not in type_priority_groups:
                type_priority_groups[key] = []
            type_priority_groups[key].append(result)

        for group_name, issues in type_priority_groups.items():
            print(f"\n{group_name.upper()} ({len(issues)} issues):")
            print("-" * 60)

            for issue in issues[:10]:  # Limit to first 10 issues per group
                print(f"  {issue.issue_key}: {issue.summary}")
                print(f"    Type: {issue.issue_type} | Priority: {issue.priority}")
                print(f"    Assignee: {issue.assignee or 'Unassigned'}")
                print(f"    Squad: {issue.squad or 'Not set'}")
                if issue.resolution_time_days is not None:
                    print(f"    Resolution Time: {issue.resolution_time_days:.1f} days")
                print(f"    Started: {issue.first_in_progress_date or 'Unknown'}")
                print(f"    Resolved: {issue.resolved_date or 'Not resolved'}")
                print()

            if len(issues) > 10:
                print(f"    ... and {len(issues) - 10} more issues")
                print()

        print("=" * 80)

    def _filter_outliers(self, times: List[float], strategy: str) -> tuple:
        """
        Filter outliers from resolution times using the specified strategy.
        IQR is preferred for skewed data, as it's less sensitive to extreme values.
        If len(times) <= 10, skip outlier filtering.
        """
        if strategy == "none" or len(times) <= 10:
            return times, 0, None

        sorted_times = sorted(times)
        original_count = len(times)

        if strategy == "trim":
            # Remove top 2% as outliers (conservative approach for datasets ~1000 issues)
            threshold_index = max(0, int(original_count * 0.98))
            filtered_times = sorted_times[:threshold_index]
            threshold = (
                sorted_times[threshold_index - 1]
                if threshold_index > 0 and threshold_index <= len(sorted_times)
                else None
            )

        elif strategy == "iqr":
            # Use Interquartile Range method (robust for skewed distributions)
            q1 = np.percentile(times, 25)
            q3 = np.percentile(times, 75)
            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            filtered_times = [t for t in times if lower <= t <= upper]
            threshold = upper

        elif strategy == "zscore":
            # Remove values with z-score > 3
            mean_val = np.mean(times)
            std_val = np.std(times)
            if std_val == 0:
                return times, 0, None
            z_scores = [(t - mean_val) / std_val for t in times]
            threshold = mean_val + 3 * std_val
            filtered_times = [times[i] for i, z in enumerate(z_scores) if abs(z) <= 3]

        else:
            # Unknown strategy, return original data
            return times, 0, None

        outliers_removed = original_count - len(filtered_times)

        # Debug logging for outlier filtering
        self.logger.debug(
            f"Outlier filtering ({strategy}): original={original_count}, "
            f"filtered={len(filtered_times)}, removed={outliers_removed}, "
            f"threshold={threshold}"
        )

        return filtered_times, outliers_removed, threshold

    # REMOVED: _calculate_robust_sla method - outdated formula (math.ceil(p95))
    # SLA calculation is now handled in _calculate_enhanced_stats using math.ceil(p90)
    # and P90 95% confidence interval upper bound for small samples (count < 30)

    def _calculate_p90_confidence_interval(
        self, times: List[float], n_bootstrap: int = 1000
    ) -> tuple:
        """
        Calculate confidence interval for P90 using bootstrap sampling.
        Using CI for small samples ensures conservative SLA targets.
        """
        bootstrap_p90s = []
        for _ in range(n_bootstrap):
            bootstrap_sample = np.random.choice(times, len(times), replace=True)
            bootstrap_p90 = np.percentile(bootstrap_sample, 90)
            bootstrap_p90s.append(bootstrap_p90)
        ci_lower = float(np.percentile(bootstrap_p90s, 2.5))
        ci_upper = float(np.percentile(bootstrap_p90s, 97.5))
        return ci_lower, ci_upper

    def _calculate_p95_confidence_interval(
        self, times: List[float], n_bootstrap: int = 1000
    ) -> tuple:
        """
        Calculate confidence interval for P95 using bootstrap sampling.

        Args:
            times: Original resolution times
            n_bootstrap: Number of bootstrap samples

        Returns:
            Tuple of (ci_lower, ci_upper) for 95% confidence interval
        """
        bootstrap_p95s = []

        for _ in range(n_bootstrap):
            bootstrap_sample = np.random.choice(times, len(times), replace=True)
            bootstrap_p95 = np.percentile(bootstrap_sample, 95)
            bootstrap_p95s.append(bootstrap_p95)

        ci_lower = float(np.percentile(bootstrap_p95s, 2.5))
        ci_upper = float(np.percentile(bootstrap_p95s, 97.5))

        return ci_lower, ci_upper

    def _calculate_resolution_buckets(self, times: List[float]) -> Dict[str, int]:
        """
        Calculate improved resolution time buckets for histogram analysis.
        Buckets: '<1d', '1–2d', '2–3d', '3–5d', '5–10d', '10–20d', '>20d'
        """
        buckets = {
            "<1d": 0,
            "1–2d": 0,
            "2–3d": 0,
            "3–5d": 0,
            "5–10d": 0,
            "10–20d": 0,
            ">20d": 0,
        }
        for time in times:
            if time < 1:
                buckets["<1d"] += 1
            elif time < 2:
                buckets["1–2d"] += 1
            elif time < 3:
                buckets["2–3d"] += 1
            elif time < 5:
                buckets["3–5d"] += 1
            elif time < 10:
                buckets["5–10d"] += 1
            elif time < 20:
                buckets["10–20d"] += 1
            else:
                buckets[">20d"] += 1
        return buckets

    def _clean_stats_output(self, stats_data: Dict, include_raw: bool = False) -> Dict:
        """
        Clean and sanitize statistics output structure.

        Args:
            stats_data: Raw statistics data
            include_raw: Whether to include raw/debugging fields

        Returns:
            Dict: Cleaned statistics data
        """
        if not stats_data:
            return {}

        cleaned_data = {}

        for key, value in stats_data.items():
            if isinstance(value, dict):
                # Handle nested dictionaries (type -> priority -> stats)
                if any(isinstance(v, dict) and "count" in v for v in value.values()):
                    # This is a priority-level dict
                    cleaned_data[key] = {}
                    for priority, stats in value.items():
                        cleaned_data[key][priority] = self._clean_individual_stats(
                            stats, include_raw
                        )
                else:
                    # This is a single stats dict
                    cleaned_data[key] = self._clean_individual_stats(value, include_raw)
            else:
                cleaned_data[key] = value

        return cleaned_data

    def _clean_individual_stats(self, stats: Dict, include_raw: bool = False) -> Dict:
        """
        Clean individual statistics object.

        Args:
            stats: Statistics dictionary
            include_raw: Whether to include raw/debugging fields

        Returns:
            Dict: Cleaned statistics
        """
        if not stats:
            return stats

        # Essential fields to keep
        essential_fields = {
            "issue_type",
            "priority",
            "count",
            "effective_count",
            "median_days",
            "p80_days",  # MOVED: This is essential for display
            "p90_days",
            "p95_days",
            "std_dev",
            "max_days",  # MOVED: This is essential for display
            "p95_trimmed_days",  # Only for selected strategy
            "suggested_sla_days",
            "sla_compliance_percentage",  # Essential for SLA tracking
            "sla_risk_level",
        }

        # Raw/debugging fields (only included if include_raw=True)
        debug_fields = {
            "outlier_strategy",
            "outliers_removed",
            "resolution_buckets",
            "median_resolution_time",  # Legacy field
            "p95_resolution_time",  # Legacy field
            "average_resolution_time",  # Legacy field
            "min_resolution_time",  # Legacy field
            "max_resolution_time",  # Legacy field
            "outlier_threshold",
            "validation_warnings",
            "diagnostic_p95_trimmed_days",
            "p95_ci_lower",
            "p95_ci_upper",
            "total_issues",  # Kept for overall stats
        }

        cleaned_stats = {}

        # Always include essential fields
        for field in essential_fields:
            if field in stats:
                cleaned_stats[field] = stats[field]

        # Include debug fields only if requested
        if include_raw:
            for field in debug_fields:
                if field in stats:
                    cleaned_stats[field] = stats[field]
        else:
            # Always include total_issues for overall stats
            if "total_issues" in stats:
                cleaned_stats["total_issues"] = stats["total_issues"]

        return cleaned_stats

    def _validate_statistics(
        self,
        median: float,
        p80: float,
        p90: float,
        p95: float,
        p95_trimmed: float,
        suggested_sla: int,
    ) -> List[str]:
        """
        Validate statistical consistency and return warnings if needed.

        Args:
            median, p80, p90, p95, p95_trimmed: Percentile values
            suggested_sla: Calculated SLA

        Returns:
            List of validation warning messages
        """
        warnings = []

        # Check percentile ordering
        if not (p95 >= p90 >= p80 >= median):
            warnings.append("Percentile ordering violated: p95 >= p90 >= p80 >= median")

        # Check trimmed vs regular P95
        if p95_trimmed > p95:
            warnings.append("P95 trimmed is greater than regular P95")

        # Check SLA vs P90
        if suggested_sla < p90:
            warnings.append("Suggested SLA is less than P90")

        return warnings
