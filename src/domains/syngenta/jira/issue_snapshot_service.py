"""
JIRA Issue Snapshot Service

This service provides functionality to fetch a snapshot of issues within a specific time window,
with comprehensive filtering options and optional comment retrieval.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from collections import defaultdict

from utils.jira.jira_assistant import JiraAssistant
from utils.logging.logging_manager import LogManager
from utils.output_manager import OutputManager


@dataclass
class IssueSnapshot:
    """Data class to hold issue snapshot information."""

    issue_key: str
    summary: str
    issue_type: str
    status: str
    status_category: str
    due_date: Optional[str]
    resolution_date: Optional[str]
    assignee: Optional[str]
    team: Optional[str]
    created_date: Optional[str]
    labels: List[str]
    components: List[str]
    description: Optional[str] = None
    comments: Optional[List[Dict]] = None


class IssueSnapshotService:
    """Service to fetch and export issue snapshots."""

    def __init__(self):
        """Initialize the service with required components."""
        self.logger = LogManager.get_instance().get_logger("IssueSnapshotService")
        self.jira_assistant = JiraAssistant()
        self.output_manager = OutputManager()

    def fetch_issue_snapshot(
        self,
        project_key: str,
        issue_type: str,
        end_date: Optional[str] = None,
        window_days: int = 7,
        teams: Optional[List[str]] = None,
        status: Optional[List[str]] = None,
        status_categories: Optional[List[str]] = None,
        include_comments: bool = False,
        text_format: str = "adf",
        verbose: bool = False,
        output_file: Optional[str] = None,
        output_formats: Optional[List[str]] = None,
    ) -> Dict:
        """
        Fetch a snapshot of issues within a specific time window.

        Args:
            project_key (str): The JIRA project key
            issue_type (str): Single issue type to fetch
            end_date (Optional[str]): Anchor date in YYYY-MM-DD format (defaults to today)
            window_days (int): Window size in days counting backwards from end_date
            teams (Optional[List[str]]): List of team names to filter by
            status (Optional[List[str]]): Status names to include (mutually exclusive with status_categories)
            status_categories (Optional[List[str]]): Status categories to include (mutually exclusive with status)
            include_comments (bool): Whether to fetch comments for each issue
            comments_format (str): Format for comment body: 'adf' (default) or 'plaintext'
            verbose (bool): Enable verbose output
            output_file (Optional[str]): Output file path for JSON export
            output_formats (Optional[List[str]]): Output formats ('console', 'json', 'md')

        Returns:
            Dict: Results with issue snapshot and metadata
        """
        try:
            self.logger.info(f"Starting issue snapshot for project {project_key}, issue type {issue_type}")

            # Parse time window
            start_date_dt, end_date_dt, end_date_str = self._parse_time_window(end_date, window_days)

            # Build JQL query
            jql_query = self._build_jql_query(
                project_key,
                issue_type,
                start_date_dt,
                end_date_dt,
                teams,
                status,
                status_categories,
            )

            self.logger.info(f"Executing JQL query: {jql_query}")

            # Fetch issues
            issues = self.jira_assistant.fetch_issues(
                jql_query=jql_query,
                fields=(
                    "key,summary,description,issuetype,status,duedate,resolutiondate,"
                    "assignee,customfield_10265,"  # Squad[Dropdown] field
                    "created,labels,components"
                ),
                max_results=100,
                expand_changelog=False,
            )

            self.logger.info(f"Fetched {len(issues)} issues")

            # Process each issue
            issue_snapshots = []
            for issue in issues:
                issue_snapshot = self._process_issue(issue, include_comments, text_format)
                issue_snapshots.append(issue_snapshot)

            # Calculate summary metrics
            metrics = self._calculate_metrics(issue_snapshots)

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

            # Prepare result
            result = {
                "project_key": project_key,
                "issue_type": issue_type,
                "time_window": {
                    "start_date": start_date_dt.strftime("%Y-%m-%d"),
                    "end_date": end_date_str,
                    "window_days": window_days,
                },
                "filters": {
                    "teams": team_label,
                    "status": status,
                    "status_categories": status_categories,
                },
                "total_issues": len(issue_snapshots),
                "metrics": metrics,
                "issues": [self._issue_to_dict(issue) for issue in issue_snapshots],
                "query_timestamp": datetime.now().isoformat(),
                "includes_comments": include_comments,
            }

            # Output handling
            if output_formats is None:
                output_formats = ["console"]

            # Generate output files
            if "json" in output_formats or output_file:
                json_path = self._save_json_output(result, output_file)
                result["output_file"] = json_path

            if "md" in output_formats:
                md_path = self._save_markdown_output(result)
                result["markdown_file"] = md_path

            # Console output
            if "console" in output_formats or verbose:
                self._print_console_output(result, verbose)

            return result

        except Exception as e:
            self.logger.error(f"Failed to fetch issue snapshot: {e}", exc_info=True)
            raise

    def _parse_time_window(self, end_date: Optional[str], window_days: int) -> tuple[datetime, datetime, str]:
        """
        Parse time window parameters.

        Returns:
            Tuple of (start_date, end_date, end_date_string)
        """
        # Parse end date
        if end_date:
            try:
                end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")
                end_date_str = end_date
            except ValueError:
                raise ValueError(f"Invalid end_date format '{end_date}'. Use YYYY-MM-DD format.")
        else:
            end_date_dt = datetime.now()
            end_date_str = end_date_dt.strftime("%Y-%m-%d")

        # Set end of day
        end_date_dt = end_date_dt.replace(hour=23, minute=59, second=59)

        # Calculate start date
        start_date_dt = end_date_dt - timedelta(days=window_days - 1)
        start_date_dt = start_date_dt.replace(hour=0, minute=0, second=0)

        self.logger.info(f"Time window: {start_date_dt.strftime('%Y-%m-%d')} to {end_date_str} ({window_days} days)")

        return start_date_dt, end_date_dt, end_date_str

    def _build_jql_query(
        self,
        project_key: str,
        issue_type: str,
        start_date: datetime,
        end_date: datetime,
        teams: Optional[List[str]],
        status: Optional[List[str]],
        status_categories: Optional[List[str]],
    ) -> str:
        """Build JQL query based on filters."""
        jql_parts = [
            f'project = "{project_key}"',
            f'issuetype = "{issue_type}"',
        ]

        # Time window filter - issues created within the window
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        jql_parts.append(f'created >= "{start_str}" AND created <= "{end_str}"')

        # Team filter (customfield_10851 is Squad[Dropdown])
        if teams:
            # Clean and deduplicate team names
            seen = set()
            cleaned = []
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

        # Status filter
        if status:
            status_conditions = " OR ".join([f'status = "{s}"' for s in status])
            jql_parts.append(f"({status_conditions})")

        # Status category filter
        if status_categories:
            category_conditions = " OR ".join([f'statusCategory = "{cat}"' for cat in status_categories])
            jql_parts.append(f"({category_conditions})")

        jql_query = " AND ".join(jql_parts)
        jql_query += " ORDER BY created DESC"

        return jql_query

    def _process_issue(self, issue: Dict, include_comments: bool, text_format: str = "adf") -> IssueSnapshot:
        """Process a single issue into IssueSnapshot data class."""
        fields = issue.get("fields", {})

        # Extract basic fields
        issue_key = issue.get("key", "")
        summary = fields.get("summary", "")
        issue_type = fields.get("issuetype", {}).get("name", "")
        status = fields.get("status", {}).get("name", "")
        status_category = fields.get("status", {}).get("statusCategory", {}).get("name", "")

        # Extract dates
        due_date = fields.get("duedate")
        resolution_date = fields.get("resolutiondate")
        created_date = fields.get("created")

        # Extract assignee
        assignee_field = fields.get("assignee")
        assignee = assignee_field.get("displayName") if assignee_field else None

        # Extract team (customfield_10265 is Squad[Dropdown])
        team = fields.get("customfield_10265", {}).get("value") if fields.get("customfield_10265") else None

        # Extract labels
        labels = fields.get("labels", [])

        # Extract components
        components = [comp.get("name", "") for comp in fields.get("components", [])]

        # Extract description (ADF or plaintext)
        description = None
        if "description" in fields:
            desc_adf = fields["description"]
            if text_format == "plaintext":
                # Use JiraAssistant's ADF to text utility
                description = self.jira_assistant._convert_adf_to_text(desc_adf)
            else:
                description = desc_adf

        # Fetch comments if requested
        comments = None
        if include_comments:
            comments = self._fetch_issue_comments(issue_key, text_format)

        return IssueSnapshot(
            issue_key=issue_key,
            summary=summary,
            issue_type=issue_type,
            status=status,
            status_category=status_category,
            due_date=due_date,
            resolution_date=resolution_date,
            assignee=assignee,
            team=team,
            created_date=created_date,
            labels=labels,
            components=components,
            description=description,
            comments=comments,
        )

    def _fetch_issue_comments(self, issue_key: str, comments_format: str = "adf") -> List[Dict]:
        """
        Fetch comments for a specific issue.

        Args:
            issue_key (str): Issue key
            comments_format (str): 'adf' (default) or 'plaintext'

        Returns:
            List[Dict]: Comments with body in requested format
        """
        try:
            comments_data = self.jira_assistant.fetch_issue_comments(issue_key)

            # If plaintext format requested, replace body with body_text
            if comments_format == "plaintext":
                for comment in comments_data:
                    if "body_text" in comment:
                        comment["body"] = comment["body_text"]
                        # Remove body_text to keep only body field
                        del comment["body_text"]
            else:
                # For ADF format, remove body_text field if present
                for comment in comments_data:
                    if "body_text" in comment:
                        del comment["body_text"]

            return comments_data
        except Exception as e:
            self.logger.warning(f"Failed to fetch comments for {issue_key}: {e}")
            return []

    def _calculate_metrics(self, issues: List[IssueSnapshot]) -> Dict:
        """Calculate summary metrics from issues."""
        from typing import DefaultDict

        status_breakdown: DefaultDict[str, int] = defaultdict(int)
        status_category_breakdown: DefaultDict[str, int] = defaultdict(int)
        team_breakdown: DefaultDict[str, int] = defaultdict(int)
        assignee_breakdown: DefaultDict[str, int] = defaultdict(int)

        with_due_date = 0
        without_due_date = 0
        resolved = 0
        unresolved = 0

        for issue in issues:
            # Status breakdown
            status_breakdown[issue.status] += 1
            status_category_breakdown[issue.status_category] += 1

            # Team breakdown
            if issue.team:
                team_breakdown[issue.team] += 1
            else:
                team_breakdown["No Team"] += 1

            # Assignee breakdown
            if issue.assignee:
                assignee_breakdown[issue.assignee] += 1
            else:
                assignee_breakdown["Unassigned"] += 1

            # Due date tracking
            if issue.due_date:
                with_due_date += 1
            else:
                without_due_date += 1

            # Resolution tracking
            if issue.resolution_date:
                resolved += 1
            else:
                unresolved += 1

        # Build metrics dictionary
        metrics = {
            "status_breakdown": dict(status_breakdown),
            "status_category_breakdown": dict(status_category_breakdown),
            "team_breakdown": dict(team_breakdown),
            "assignee_breakdown": dict(assignee_breakdown),
            "with_due_date": with_due_date,
            "without_due_date": without_due_date,
            "resolved": resolved,
            "unresolved": unresolved,
        }

        return metrics

    def _issue_to_dict(self, issue: IssueSnapshot) -> Dict:
        """Convert IssueSnapshot to dictionary."""
        from typing import Any

        issue_dict: Dict[str, Any] = {
            "issue_key": issue.issue_key,
            "summary": issue.summary,
            "issue_type": issue.issue_type,
            "status": issue.status,
            "status_category": issue.status_category,
            "due_date": issue.due_date,
            "resolution_date": issue.resolution_date,
            "assignee": issue.assignee,
            "team": issue.team,
            "created_date": issue.created_date,
            "labels": issue.labels,
            "components": issue.components,
        }

        if issue.description is not None:
            issue_dict["description"] = issue.description
        if issue.comments is not None:
            issue_dict["comments"] = issue.comments

        return issue_dict

    def _save_json_output(self, result: Dict, output_file: Optional[str] = None) -> str:
        """Save results to JSON file using OutputManager."""
        project_key = result["project_key"]
        issue_type = result["issue_type"].lower()

        # Create subdirectory following the pattern: issue-snapshot_YYYYMMDD
        sub_dir = f"issue-snapshot_{datetime.now().strftime('%Y%m%d')}"
        file_basename = f"snapshot_{project_key}_{issue_type}"

        # Use OutputManager to save JSON (per-day folder in output/)
        json_path = self.output_manager.save_json_report(
            data=result,
            sub_dir=sub_dir,
            file_basename=file_basename,
            output_path=output_file,  # Will use custom path if provided
        )

        self.logger.info(f"JSON output saved to: {json_path}")
        return json_path

    def _save_markdown_output(self, result: Dict, output_file: Optional[str] = None) -> str:
        """Save results to Markdown file using OutputManager."""
        project_key = result["project_key"]
        issue_type = result["issue_type"].lower()

        # Create subdirectory following the pattern: issue-snapshot_YYYYMMDD
        sub_dir = f"issue-snapshot_{datetime.now().strftime('%Y%m%d')}"
        file_basename = f"snapshot_{project_key}_{issue_type}"

        # Generate markdown content
        markdown_content = self._generate_markdown(result)

        # Use OutputManager to save Markdown (per-day folder in output/)
        md_path = self.output_manager.save_markdown_report(
            content=markdown_content,
            sub_dir=sub_dir,
            file_basename=file_basename,
            output_path=output_file,  # Will use custom path if provided
        )

        self.logger.info(f"Markdown output saved to: {md_path}")
        return md_path

    def _generate_markdown(self, result: Dict) -> str:
        """Generate Markdown content from results."""
        lines = []

        # Header
        lines.append(f"# JIRA Issue Snapshot - {result['project_key']}")
        lines.append("")
        lines.append(f"**Issue Type:** {result['issue_type']}")
        lines.append(
            f"**Time Window:** {result['time_window']['start_date']} to {result['time_window']['end_date']} "
            f"({result['time_window']['window_days']} days)"
        )
        lines.append(f"**Generated:** {result['query_timestamp']}")
        lines.append("")

        # Filters
        if result["filters"]["teams"]:
            lines.append(f"**Teams:** {result['filters']['teams']}")
        if result["filters"]["status"]:
            lines.append(f"**Status Filter:** {', '.join(result['filters']['status'])}")
        if result["filters"]["status_categories"]:
            lines.append(f"**Status Categories:** {', '.join(result['filters']['status_categories'])}")
        lines.append("")

        # Summary
        lines.append("## Summary")
        lines.append("")
        lines.append(f"**Total Issues:** {result['total_issues']}")
        lines.append(f"**With Due Date:** {result['metrics']['with_due_date']}")
        lines.append(f"**Without Due Date:** {result['metrics']['without_due_date']}")
        lines.append(f"**Resolved:** {result['metrics']['resolved']}")
        lines.append(f"**Unresolved:** {result['metrics']['unresolved']}")
        lines.append("")

        # Status Breakdown
        lines.append("## Status Breakdown")
        lines.append("")
        for status, count in sorted(result["metrics"]["status_breakdown"].items(), key=lambda x: x[1], reverse=True):
            lines.append(f"- **{status}:** {count}")
        lines.append("")

        # Team Breakdown
        if result["metrics"]["team_breakdown"]:
            lines.append("## Team Breakdown")
            lines.append("")
            for team, count in sorted(result["metrics"]["team_breakdown"].items(), key=lambda x: x[1], reverse=True):
                lines.append(f"- **{team}:** {count}")
            lines.append("")

        # Issues Table
        lines.append("## Issues")
        lines.append("")
        lines.append("| Key | Summary | Status | Team | Assignee | Due Date | Created | Resolution Date |")
        lines.append("|-----|---------|--------|------|----------|----------|---------|-----------------|")

        for issue in result["issues"]:
            key = issue["issue_key"]
            summary = issue["summary"][:50] + "..." if len(issue["summary"]) > 50 else issue["summary"]
            status = issue["status"]
            team = issue["team"] or "N/A"
            assignee = issue["assignee"] or "Unassigned"
            due_date = issue["due_date"] or "N/A"
            created = issue["created_date"][:10] if issue["created_date"] else "N/A"
            resolution = issue["resolution_date"][:10] if issue["resolution_date"] else "N/A"

            lines.append(
                f"| {key} | {summary} | {status} | {team} | {assignee} | {due_date} | {created} | {resolution} |"
            )

        lines.append("")

        # Issue Details with Descriptions
        lines.append("## Issue Details")
        lines.append("")
        for issue in result["issues"]:
            lines.append(f"### {issue['issue_key']}: {issue['summary']}")
            lines.append("")
            lines.append(f"- **Status:** {issue['status']}")
            lines.append(f"- **Team:** {issue['team'] or 'N/A'}")
            lines.append(f"- **Assignee:** {issue['assignee'] or 'Unassigned'}")
            lines.append(f"- **Due Date:** {issue['due_date'] or 'N/A'}")
            lines.append(f"- **Created:** {issue['created_date'][:10] if issue['created_date'] else 'N/A'}")
            lines.append(
                f"- **Resolution Date:** {issue['resolution_date'][:10] if issue['resolution_date'] else 'N/A'}"
            )

            if issue.get("description"):
                lines.append("")
                lines.append("**Description:**")
                lines.append("")
                # If description is a string (plaintext), use it directly
                # If it's a dict (ADF), it will be serialized as is
                desc = issue["description"]
                if isinstance(desc, str):
                    lines.append(f"> {desc}")
                else:
                    lines.append("> (Description in ADF format - see JSON output)")
                lines.append("")
            else:
                lines.append("")
                lines.append("_No description provided_")
                lines.append("")

            lines.append("")

        # Comments section if included
        if result["includes_comments"]:
            lines.append("## Comments")
            lines.append("")
            for issue in result["issues"]:
                if issue.get("comments"):
                    lines.append(f"### {issue['issue_key']}: {issue['summary']}")
                    lines.append("")
                    for comment in issue["comments"]:
                        author = comment.get("author", "Unknown")
                        created = comment.get("created", "Unknown")
                        # Use body field directly (will be plaintext or ADF depending on comments_format parameter)
                        body = comment.get("body", "")
                        lines.append(f"**{author}** - {created}")
                        lines.append(f"> {body}")
                        lines.append("")

        return "\n".join(lines)

    def _print_console_output(self, result: Dict, verbose: bool):
        """Print results to console."""
        print("\n" + "=" * 80)
        print(f"JIRA Issue Snapshot - {result['project_key']}")
        print("=" * 80)
        print(f"\nIssue Type: {result['issue_type']}")
        print(
            f"Time Window: {result['time_window']['start_date']} to {result['time_window']['end_date']} "
            f"({result['time_window']['window_days']} days)"
        )

        if result["filters"]["teams"]:
            print(f"Teams: {result['filters']['teams']}")
        if result["filters"]["status"]:
            print(f"Status Filter: {', '.join(result['filters']['status'])}")
        if result["filters"]["status_categories"]:
            print(f"Status Categories: {', '.join(result['filters']['status_categories'])}")

        print(f"\n{'=' * 80}")
        print("SUMMARY")
        print("=" * 80)
        print(f"Total Issues: {result['total_issues']}")
        print(f"With Due Date: {result['metrics']['with_due_date']}")
        print(f"Without Due Date: {result['metrics']['without_due_date']}")
        print(f"Resolved: {result['metrics']['resolved']}")
        print(f"Unresolved: {result['metrics']['unresolved']}")

        print(f"\n{'=' * 80}")
        print("STATUS BREAKDOWN")
        print("=" * 80)
        for status, count in sorted(result["metrics"]["status_breakdown"].items(), key=lambda x: x[1], reverse=True):
            print(f"  {status}: {count}")

        if result["metrics"]["team_breakdown"]:
            print(f"\n{'=' * 80}")
            print("TEAM BREAKDOWN")
            print("=" * 80)
            for team, count in sorted(result["metrics"]["team_breakdown"].items(), key=lambda x: x[1], reverse=True):
                print(f"  {team}: {count}")

        if verbose:
            print(f"\n{'=' * 80}")
            print("ISSUES")
            print("=" * 80)
            for issue in result["issues"]:
                print(f"\n{issue['issue_key']}: {issue['summary']}")
                print(f"  Status: {issue['status']} ({issue['status_category']})")
                print(f"  Team: {issue['team'] or 'N/A'}")
                print(f"  Assignee: {issue['assignee'] or 'Unassigned'}")
                print(f"  Due Date: {issue['due_date'] or 'N/A'}")
                print(f"  Created: {issue['created_date']}")
                print(f"  Resolution: {issue['resolution_date'] or 'N/A'}")

                if issue.get("comments"):
                    print(f"  Comments: {len(issue['comments'])}")

        print("\n" + "=" * 80 + "\n")
