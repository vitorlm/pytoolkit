import os
from datetime import date, datetime
from typing import Any

import requests

from utils.data.json_manager import JSONManager
from utils.jira.jira_assistant import JiraAssistant
from utils.logging.logging_manager import LogManager


class IssueWithoutDueDate:
    """Represents an issue without due date."""

    # Class-level cache for the mapping to avoid reading the file multiple times
    _jira_to_slack_mapping = None
    _mapping_file_path = None

    def __init__(self, issue_data: dict):
        self.key = issue_data.get("key", "")
        self.summary = issue_data.get("fields", {}).get("summary", "")
        self.issue_type = issue_data.get("fields", {}).get("issuetype", {}).get("name", "")
        self.status = issue_data.get("fields", {}).get("status", {}).get("name", "")
        self.priority = self._get_priority(issue_data.get("fields", {}))
        self.created_date = self._parse_date(issue_data.get("fields", {}).get("created"))
        self.updated_date = self._parse_date(issue_data.get("fields", {}).get("updated"))

        # Extract assignee information
        assignee = issue_data.get("fields", {}).get("assignee")
        self.assignee_id = assignee.get("accountId", "") if assignee else ""
        self.assignee_name = assignee.get("displayName", "") if assignee else ""
        self.assignee_slack_id = self._get_slack_user_id(self.assignee_id) if self.assignee_id else ""

        # Extract squad information from custom field
        self.squad = self._get_squad(issue_data.get("fields", {}))

        # Calculate days in progress
        self.days_in_progress = self._calculate_days_in_progress()

    @classmethod
    def _load_jira_to_slack_mapping(cls) -> dict[str, str]:
        """Load JIRA to Slack user ID mapping from JSON file."""
        if cls._jira_to_slack_mapping is None:
            # Get mapping file path from environment or use default
            if cls._mapping_file_path is None:
                cls._mapping_file_path = os.path.join(os.path.dirname(__file__), "jira_to_slack_user_mapping.json")

            try:
                cls._jira_to_slack_mapping = JSONManager.read_json(cls._mapping_file_path, default={})
            except Exception as e:
                # Log warning but don't fail - return empty mapping
                print(f"Warning: Could not load JIRA to Slack mapping file: {e}")
                cls._jira_to_slack_mapping = {}

        return cls._jira_to_slack_mapping

    def _get_slack_user_id(self, jira_user_id: str) -> str:
        """Map JIRA user ID to Slack user ID using the mapping file."""
        if not jira_user_id:
            return ""

        mapping = self._load_jira_to_slack_mapping()
        return mapping.get(jira_user_id, "")

    def _parse_date(self, date_str: str | None) -> date | None:
        """Parse date string to date object."""
        if not date_str:
            return None
        try:
            # Handle different date formats
            if "T" in date_str:
                return datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()
            else:
                return datetime.strptime(date_str, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None

    def _get_priority(self, fields: dict) -> str:
        """Extract priority from fields."""
        priority = fields.get("priority")
        if priority:
            return priority.get("name", "")
        return ""

    def _get_squad(self, fields: dict) -> str:
        """Extract squad from custom field."""
        # Squad[Dropdown] is typically stored in customfield_10851
        squad_field = fields.get("customfield_10851")
        if squad_field:
            if isinstance(squad_field, dict):
                return squad_field.get("value", "")
            elif isinstance(squad_field, str):
                return squad_field
        return ""

    def _calculate_days_in_progress(self) -> int:
        """Calculate how many days the issue has been in progress."""
        if not self.updated_date:
            return 0

        today = date.today()
        return (today - self.updated_date).days


class IssueDueDateMonitorService:
    """Service for monitoring issues without due dates."""

    def __init__(self, slack_webhook_url: str | None = None):
        """Initialize the service."""
        self.logger = LogManager.get_instance().get_logger("IssueDueDateMonitorService")
        self.jira_assistant = JiraAssistant()
        self.slack_webhook_url = slack_webhook_url
        self.slack_service = SlackNotificationService(slack_webhook_url) if slack_webhook_url else None

    def run_issue_check(
        self,
        squad: str,
        project_key: str = "CWS",
        issue_types: list[str] | None = None,
        dry_run: bool = False,
    ) -> bool:
        """Run the issue check for a specific squad.

        Args:
            squad (str): Squad name to monitor
            project_key (str): JIRA project key
            issue_types (Optional[List[str]]): List of issue types to filter (e.g., ['Story', 'Task', 'Bug'])
            dry_run (bool): If True, don't send notifications

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            issue_types_msg = f" (issue types: {', '.join(issue_types)})" if issue_types else " (all issue types)"
            self.logger.info(f"Starting issue due date monitoring for squad '{squad}'{issue_types_msg}")

            # Fetch issues without due dates
            issues_without_duedate = self._fetch_issues_without_duedate(squad, project_key, issue_types)

            if not issues_without_duedate:
                self.logger.info(f"No issues without due dates found for squad '{squad}'{issue_types_msg}")
                return True

            self.logger.info(f"Found {len(issues_without_duedate)} issues without due dates")

            # Log findings
            self._log_issue_details(issues_without_duedate)

            # Send notifications if not in dry run mode
            if not dry_run and self.slack_service:
                notification_sent = self.slack_service.send_issues_notification(issues_without_duedate, squad)

                if notification_sent:
                    self.logger.info("Slack notification sent successfully")
                else:
                    self.logger.error("Failed to send Slack notification")
                    return False
            elif dry_run:
                self.logger.info("Dry run mode - notifications not sent")
            else:
                self.logger.warning("No Slack webhook configured - notifications not sent")

            return True

        except Exception as e:
            self.logger.error(f"Error in issue due date monitoring: {e}", exc_info=True)
            return False

    def _fetch_issues_without_duedate(
        self, squad: str, project_key: str, issue_types: list[str] | None = None
    ) -> list[IssueWithoutDueDate]:
        """Fetch issues that are in progress but don't have due dates.

        Args:
            squad (str): Squad name to filter
            project_key (str): JIRA project key
            issue_types (Optional[List[str]]): List of issue types to filter

        Returns:
            List[IssueWithoutDueDate]: List of issues without due dates
        """
        try:
            # Build JQL query to find issues in progress without due dates
            jql_parts = [
                f'project = "{project_key}"',
                'statusCategory = "In Progress"',
                "duedate is EMPTY",
                f'"Squad[Dropdown]" = "{squad}"',
            ]

            # Add issue type filter if specified
            if issue_types:
                issue_types_str = '", "'.join(issue_types)
                jql_parts.append(f'issuetype IN ("{issue_types_str}")')

            jql_query = " AND ".join(jql_parts) + " ORDER BY updated DESC"

            self.logger.info(f"Executing JQL query: {jql_query}")

            # Fetch issues from JIRA
            issues = self.jira_assistant.fetch_issues(
                jql_query=jql_query,
                fields=("key,summary,issuetype,status,priority,created,updated,assignee,customfield_10851,duedate"),
                max_results=100,
                expand_changelog=False,
            )

            # Convert to our domain objects
            issues_without_duedate = []
            for issue in issues:
                issue_obj = IssueWithoutDueDate(issue)
                issues_without_duedate.append(issue_obj)

            return issues_without_duedate

        except Exception as e:
            self.logger.error(f"Error fetching issues without due dates: {e}", exc_info=True)
            raise

    def _log_issue_details(self, issues: list[IssueWithoutDueDate]) -> None:
        """Log details about the issues found."""
        self.logger.info("Issues without due dates found:")

        for issue in issues:
            assignee_info = f" (assigned to {issue.assignee_name})" if issue.assignee_name else " (unassigned)"
            self.logger.info(
                f"  - {issue.key}: {issue.summary[:60]}... "
                f"[{issue.issue_type}] [{issue.status}] "
                f"[{issue.days_in_progress} days in progress]"
                f"{assignee_info}"
            )


class SlackNotificationService:
    """Service for sending Slack notifications about issues without due dates."""

    def __init__(self, webhook_url: str | None):
        """Initialize with Slack webhook URL."""
        self.webhook_url = webhook_url
        self.logger = LogManager.get_instance().get_logger("SlackNotificationService")

    def send_issues_notification(
        self,
        issues: list[IssueWithoutDueDate],
        squad: str,
        slack_token: str | None = None,
        recipient_id: str | None = None,
    ) -> bool:
        """Send a Slack notification about issues without due dates.

        Args:
            issues: List of issues without due dates
            squad: Squad name
            slack_token: Optional Slack bot token
            recipient_id: Optional recipient channel/user ID

        Returns:
            bool: True if successful, False otherwise
        """
        if not issues:
            self.logger.info("No issues without due dates to report")
            return True

        # Use environment variables if not provided
        token = slack_token or os.getenv("SLACK_BOT_TOKEN")
        channel = recipient_id or os.getenv("SLACK_CHANNEL_ID")

        if not token:
            self.logger.error("Slack token not provided and SLACK_BOT_TOKEN not set")
            return False

        if not channel:
            self.logger.error("Slack channel not provided and SLACK_CHANNEL_ID not set")
            return False

        blocks = self._format_issues_blocks(issues, squad)

        try:
            payload = {
                "channel": channel,
                "blocks": blocks,
                "text": f"{len(issues)} issues without due dates found in {squad} squad.",
            }

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            }

            response = requests.post(
                "https://slack.com/api/chat.postMessage",
                json=payload,
                headers=headers,
                timeout=30,
            )

            if response.status_code == 200 and response.json().get("ok"):
                self.logger.info(f"Successfully sent notification for {len(issues)} issues without due dates")
                return True
            else:
                self.logger.error(f"Failed to send Slack notification. Response: {response.text}")
                return False

        except Exception as e:
            self.logger.error(f"Error sending Slack notification: {e}", exc_info=True)
            return False

    def _format_issues_blocks(self, issues: list[IssueWithoutDueDate], squad: str) -> list[dict[str, Any]]:
        """Format issues into Slack Block Kit format."""
        blocks = []

        # Header
        blocks.append(
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"⏰ Issues Missing Due Dates - {squad} Squad",
                },
            }
        )

        # Brief intro
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{len(issues)}* issues in progress need due dates assigned:",
                },
            }
        )

        blocks.append({"type": "divider"})

        # Group issues by priority
        priority_groups = self._group_issues_by_priority(issues)

        for priority, priority_issues in priority_groups.items():
            if not priority_issues:
                continue

            # Priority header
            priority_emoji = self._get_priority_emoji(priority)
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{priority_emoji} {priority} Priority ({len(priority_issues)} issues)*",
                    },
                }
            )

            # List issues for this priority
            for issue in priority_issues[:5]:  # Limit to 5 issues per priority to avoid message size limits
                assignee_mention = ""
                if issue.assignee_slack_id:
                    assignee_mention = f" • <@{issue.assignee_slack_id}>"
                elif issue.assignee_name:
                    assignee_mention = f" • {issue.assignee_name}"

                blocks.append(
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": (
                                f"• *<https://digital-product-engineering.atlassian.net/browse/{issue.key}|{issue.key}>*: "
                                f"{issue.summary[:80]}{'...' if len(issue.summary) > 80 else ''}\n"
                                f"  _{issue.issue_type}_ • _{issue.status}_ • "
                                f"_{issue.days_in_progress} days in progress_{assignee_mention}"
                            ),
                        },
                    }
                )

            if len(priority_issues) > 5:
                blocks.append(
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"_... and {len(priority_issues) - 5} more {priority.lower()} priority issues_",
                        },
                    }
                )

        # Footer with action items
        blocks.append({"type": "divider"})
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "📋 *Action Required:*\n"
                        "• Review each issue and assign appropriate due dates\n"
                        "• Consider if any issues can be completed quickly\n"
                        "• Escalate blocked issues to the team lead"
                    ),
                },
            }
        )

        return blocks

    def _group_issues_by_priority(self, issues: list[IssueWithoutDueDate]) -> dict[str, list[IssueWithoutDueDate]]:
        """Group issues by priority."""
        priority_order = ["Highest", "High", "Medium", "Low", "Lowest", ""]
        groups: dict[str, list[IssueWithoutDueDate]] = {priority: [] for priority in priority_order}

        for issue in issues:
            priority = issue.priority if issue.priority in priority_order else ""
            groups[priority].append(issue)

        # Sort issues within each priority by days in progress (descending)
        for priority_issues in groups.values():
            priority_issues.sort(key=lambda x: x.days_in_progress, reverse=True)

        return groups

    def _get_priority_emoji(self, priority: str) -> str:
        """Get emoji for priority level."""
        priority_emojis = {
            "Highest": "🔴",
            "High": "🟠",
            "Medium": "🟡",
            "Low": "🟢",
            "Lowest": "🔵",
            "": "⚪",
        }
        return priority_emojis.get(priority, "⚪")
