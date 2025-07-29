"""
JIRA Open Issues Service

This service provides functionality to fetch all currently open issues from a JIRA project
without date filtering. It provides a snapshot of all active work items at the current moment.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from utils.data.json_manager import JSONManager
from utils.jira.jira_assistant import JiraAssistant
from utils.logging.logging_manager import LogManager
from utils.output_manager import OutputManager


@dataclass
class OpenIssueResult:
    """Data class to hold open issue information."""

    issue_key: str
    summary: str
    issue_type: str
    status: str
    status_category: str
    priority: Optional[str]
    assignee: Optional[str]
    team: Optional[str]
    created_date: Optional[str]
    due_date: Optional[str]
    labels: List[str]
    components: List[str]


class OpenIssuesService:
    """Service to fetch and analyze currently open issues."""

    def __init__(self):
        """Initialize the service with required components."""
        self.logger = LogManager.get_instance().get_logger("OpenIssuesService")
        self.jira_assistant = JiraAssistant()
        self.output_manager = OutputManager()

    def fetch_open_issues(
        self,
        project_key: str,
        issue_types: List[str],
        team: Optional[str] = None,
        status_categories: Optional[List[str]] = None,
        verbose: bool = False,
        output_file: Optional[str] = None,
    ) -> Optional[Dict]:
        """
        Fetch all currently open issues from a JIRA project.

        Args:
            project_key (str): The JIRA project key
            issue_types (List[str]): List of issue types to include
            team (Optional[str]): Team filter using Squad[Dropdown] field
            status_categories (List[str]): List of status categories to include
            verbose (bool): Enable verbose output
            output_file (Optional[str]): Output file path

        Returns:
            Dict: Results with open issues and summary metrics
        """
        try:
            self.logger.info(f"Starting open issues retrieval for project {project_key}")

            # Default status categories if not provided
            if status_categories is None:
                status_categories = ["To Do", "In Progress"]

            # Build JQL query
            jql_query = self._build_jql_query(
                project_key, issue_types, team, status_categories
            )

            self.logger.info(f"Executing JQL query: {jql_query}")

            # Fetch issues
            issues = self.jira_assistant.fetch_issues(
                jql_query=jql_query,
                fields=(
                    "key,summary,issuetype,status,priority,duedate,"
                    "created,assignee,customfield_10851,customfield_10265,"
                    "labels,components"
                ),
                max_results=1000,
                expand_changelog=False,
            )

            self.logger.info(f"Fetched {len(issues)} open issues")

            # Process each issue
            open_issues = []
            for issue in issues:
                issue_result = self._process_issue(issue)
                open_issues.append(issue_result)

            # Calculate summary metrics
            metrics = self._calculate_metrics(open_issues)

            # Prepare result
            result = {
                "project_key": project_key,
                "issue_types": issue_types,
                "status_categories": status_categories,
                "team": team,
                "total_issues": len(open_issues),
                "status_breakdown": metrics["status_breakdown"],
                "type_breakdown": metrics["type_breakdown"],
                "priority_breakdown": metrics["priority_breakdown"],
                "team_breakdown": metrics["team_breakdown"],
                "issues": [self._issue_to_dict(issue) for issue in open_issues],
                "query_timestamp": datetime.now().isoformat(),
            }

            # Output verbose information
            if verbose:
                self._print_verbose_output(open_issues)

            # Save to file if requested
            if output_file:
                self._save_to_file(result, output_file)

            return result

        except Exception as e:
            self.logger.error(f"Failed to fetch open issues: {e}")
            raise

    def _build_jql_query(
        self,
        project_key: str,
        issue_types: List[str],
        team: Optional[str],
        status_categories: List[str],
    ) -> str:
        """
        Build JQL query to fetch open issues.

        Args:
            project_key (str): The JIRA project key
            issue_types (List[str]): List of issue types
            team (Optional[str]): Team filter
            status_categories (List[str]): List of status categories

        Returns:
            str: JQL query string
        """
        # Base query
        jql_parts = [f'project = "{project_key}"']

        # Add issue types
        if issue_types:
            issue_types_str = '", "'.join(issue_types)
            jql_parts.append(f'issuetype IN ("{issue_types_str}")')

        # Add status categories
        if status_categories:
            status_categories_str = '", "'.join(status_categories)
            jql_parts.append(f'statusCategory IN ("{status_categories_str}")')

        # Add team filter if specified
        if team:
            jql_parts.append(f'"Squad[Dropdown]" = "{team}"')

        # Order by created date (newest first)
        jql_query = " AND ".join(jql_parts) + " ORDER BY created DESC"

        return jql_query

    def _process_issue(self, issue: Dict) -> OpenIssueResult:
        """
        Process a single issue and extract relevant information.

        Args:
            issue (Dict): Issue data from JIRA

        Returns:
            OpenIssueResult: Processed issue information
        """
        fields = issue.get("fields", {})

        # Extract basic information
        issue_key = issue.get("key", "")
        summary = fields.get("summary", "")
        issue_type = fields.get("issuetype", {}).get("name", "")
        status = fields.get("status", {}).get("name", "")
        status_category = fields.get("status", {}).get("statusCategory", {}).get("name", "")

        # Extract optional fields
        priority = fields.get("priority", {}).get("name") if fields.get("priority") else None
        assignee_field = fields.get("assignee")
        assignee = assignee_field.get("displayName") if assignee_field else None

        # Extract team information (Squad[Dropdown] field)
        team_field = fields.get("customfield_10851")  # Squad[Dropdown]
        team = team_field.get("value") if team_field else None

        # Extract dates
        created_date = fields.get("created")
        due_date = fields.get("duedate")

        # Extract labels
        labels = fields.get("labels", [])

        # Extract components
        components = [comp.get("name", "") for comp in fields.get("components", [])]

        return OpenIssueResult(
            issue_key=issue_key,
            summary=summary,
            issue_type=issue_type,
            status=status,
            status_category=status_category,
            priority=priority,
            assignee=assignee,
            team=team,
            created_date=created_date,
            due_date=due_date,
            labels=labels,
            components=components,
        )

    def _calculate_metrics(self, open_issues: List[OpenIssueResult]) -> Dict:
        """
        Calculate summary metrics for open issues.

        Args:
            open_issues (List[OpenIssueResult]): List of processed issues

        Returns:
            Dict: Summary metrics
        """
        status_breakdown: Dict[str, int] = {}
        type_breakdown: Dict[str, int] = {}
        priority_breakdown: Dict[str, int] = {}
        team_breakdown: Dict[str, int] = {}

        for issue in open_issues:
            # Status breakdown
            status_breakdown[issue.status] = status_breakdown.get(issue.status, 0) + 1

            # Type breakdown
            type_breakdown[issue.issue_type] = type_breakdown.get(issue.issue_type, 0) + 1

            # Priority breakdown
            priority = issue.priority or "No Priority"
            priority_breakdown[priority] = priority_breakdown.get(priority, 0) + 1

            # Team breakdown
            team = issue.team or "No Team"
            team_breakdown[team] = team_breakdown.get(team, 0) + 1

        return {
            "status_breakdown": status_breakdown,
            "type_breakdown": type_breakdown,
            "priority_breakdown": priority_breakdown,
            "team_breakdown": team_breakdown,
        }

    def _issue_to_dict(self, issue: OpenIssueResult) -> Dict:
        """
        Convert OpenIssueResult to dictionary for JSON serialization.

        Args:
            issue (OpenIssueResult): Issue result object

        Returns:
            Dict: Issue data as dictionary
        """
        return {
            "issue_key": issue.issue_key,
            "summary": issue.summary,
            "issue_type": issue.issue_type,
            "status": issue.status,
            "status_category": issue.status_category,
            "priority": issue.priority,
            "assignee": issue.assignee,
            "team": issue.team,
            "created_date": issue.created_date,
            "due_date": issue.due_date,
            "labels": issue.labels,
            "components": issue.components,
        }

    def _print_verbose_output(self, open_issues: List[OpenIssueResult]):
        """
        Print detailed information about open issues.

        Args:
            open_issues (List[OpenIssueResult]): List of processed issues
        """
        print("\n" + "=" * 80)
        print("DETAILED OPEN ISSUES")
        print("=" * 80)

        for issue in open_issues:
            print(f"\n🎫 {issue.issue_key}: {issue.summary}")
            print(f"   Type: {issue.issue_type} | Status: {issue.status}")
            if issue.priority:
                print(f"   Priority: {issue.priority}")
            if issue.assignee:
                print(f"   Assignee: {issue.assignee}")
            if issue.team:
                print(f"   Team: {issue.team}")
            if issue.due_date:
                print(f"   Due Date: {issue.due_date}")
            if issue.labels:
                print(f"   Labels: {', '.join(issue.labels)}")
            if issue.components:
                print(f"   Components: {', '.join(issue.components)}")

    def _save_to_file(self, result: Dict, output_file: str):
        """
        Save results to a JSON file.

        Args:
            result (Dict): Results data
            output_file (str): Output file path
        """
        try:
            JSONManager.write_json(result, output_file)
            self.logger.info(f"Results saved to {output_file}")
        except Exception as e:
            self.logger.error(f"Failed to save results to file: {e}")
            raise
