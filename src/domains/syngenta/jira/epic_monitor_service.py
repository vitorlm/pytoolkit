from typing import Any, Dict, List, Optional
from datetime import date, datetime, timedelta
import requests
import re
import os
from utils.jira.jira_assistant import JiraAssistant
from utils.logging.logging_manager import LogManager
from utils.jira.error import JiraManagerError
from utils.data.json_manager import JSONManager


class EpicIssue:
    """Represents an epic issue with its problems."""

    # Class-level cache for the mapping to avoid reading the file multiple times
    _jira_to_slack_mapping = None
    _mapping_file_path = None

    def __init__(self, epic_data: Dict):
        self.key = epic_data.get("key", "")
        self.summary = epic_data.get("fields", {}).get("summary", "")
        self.status = epic_data.get("fields", {}).get("status", {}).get("name", "")
        self.start_date = self._parse_date(epic_data.get("fields", {}).get("customfield_10015"))
        self.due_date = self._parse_date(epic_data.get("fields", {}).get("duedate"))
        self.fix_version = self._get_fix_version(epic_data.get("fields", {}).get("fixVersions", []))
        assignee = epic_data.get("fields", {}).get("assignee")
        self.assignee_id = assignee.get("accountId", "") if assignee else ""
        self.assignee_name = assignee.get("displayName", "") if assignee else ""
        self.assignee_slack_id = (
            self._get_slack_user_id(self.assignee_id) if self.assignee_id else ""
        )
        self.problems = []

    @classmethod
    def _load_jira_to_slack_mapping(cls) -> Dict[str, str]:
        """Load JIRA to Slack user ID mapping from JSON file."""
        if cls._jira_to_slack_mapping is None:
            # Get mapping file path from environment or use default
            if cls._mapping_file_path is None:
                cls._mapping_file_path = (
                    os.path.join(os.path.dirname(__file__), "jira_to_slack_user_mapping.json"),
                )

            try:
                cls._jira_to_slack_mapping = JSONManager.read_json(
                    cls._mapping_file_path, default={}
                )
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

    @classmethod
    def reload_mapping(cls, mapping_file_path: Optional[str] = None):
        """Force reload of the JIRA to Slack mapping file."""
        if mapping_file_path:
            cls._mapping_file_path = mapping_file_path
        cls._jira_to_slack_mapping = None
        cls._load_jira_to_slack_mapping()

    def _parse_date(self, date_str: Optional[str]) -> Optional[date]:
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

    def _get_fix_version(self, fix_versions: List[Dict]) -> Optional[str]:
        """Extract fix version from the list."""
        if not fix_versions:
            return None
        return fix_versions[0].get("name", "")


class CycleDetector:
    """Detects and validates current cycle based on configurable year start and week counting."""

    # Each quarter has 13 weeks total: C1 (6 weeks) + C2 (7 weeks)
    CYCLE_WEEKS = {"C1": 6, "C2": 7}
    QUARTER_TOTAL_WEEKS = 13

    @classmethod
    def _get_year_start_date(cls) -> date:
        """Get the configured year start date from environment or use default."""
        year_start_str = os.getenv("YEAR_START_DATE", "2025-01-06")
        try:
            return datetime.strptime(year_start_str, "%Y-%m-%d").date()
        except ValueError:
            # Fallback to January 6, 2025 if invalid format
            return date(2025, 1, 6)

    @classmethod
    def _calculate_cycle_dates(cls, year_start: date) -> Dict[str, Dict[str, date]]:
        """Calculate all cycle start and end dates for the year."""
        cycles = {}
        current_date = year_start

        for quarter in range(1, 5):  # Q1, Q2, Q3, Q4
            # C1 cycle (6 weeks)
            c1_start = current_date
            c1_end = current_date + timedelta(weeks=cls.CYCLE_WEEKS["C1"]) - timedelta(days=1)
            cycles[f"Q{quarter}C1"] = {"start": c1_start, "end": c1_end}

            # C2 cycle (7 weeks)
            c2_start = c1_end + timedelta(days=1)
            c2_end = c2_start + timedelta(weeks=cls.CYCLE_WEEKS["C2"]) - timedelta(days=1)
            cycles[f"Q{quarter}C2"] = {"start": c2_start, "end": c2_end}

            # Move to next quarter
            current_date = c2_end + timedelta(days=1)

        return cycles

    @classmethod
    def get_current_cycle(cls) -> str:
        """Get current cycle based on current date and configured year start."""
        today = date.today()
        year_start = cls._get_year_start_date()
        cycle_dates = cls._calculate_cycle_dates(year_start)

        for cycle_name, dates in cycle_dates.items():
            if dates["start"] <= today <= dates["end"]:
                return cycle_name

        # If no cycle found, we might be in the next year or before year start
        # Return the closest cycle
        if today < year_start:
            return "Q1C1"  # Before year start
        else:
            return "Q4C2"  # After year end

    @classmethod
    def is_fix_version_current_cycle(cls, fix_version: Optional[str]) -> bool:
        """Check if fix version matches current cycle pattern."""
        if not fix_version:
            return False

        current_cycle = cls.get_current_cycle()
        current_year = cls._get_year_start_date().year

        # Extract cycle pattern from fix version (e.g., "QI 2025/Q2 C2" -> "Q2C2")
        # Handle different patterns like "Q2 C2", "Q2C2", etc.
        pattern = r"Q(\d)\s*C(\d)"
        match = re.search(pattern, fix_version.upper())

        if match:
            quarter = match.group(1)
            cycle = match.group(2)
            version_cycle = f"Q{quarter}C{cycle}"

            # Also check year if present
            year_match = re.search(r"(\d{4})", fix_version)
            if year_match:
                version_year = int(year_match.group(1))
                return version_cycle == current_cycle and version_year == current_year
            else:
                return version_cycle == current_cycle

        return False

    @classmethod
    def get_cycle_info(cls, cycle_name: Optional[str] = None) -> Dict[str, date]:
        """Get start and end dates for a specific cycle or current cycle."""
        if not cycle_name:
            cycle_name = cls.get_current_cycle()

        year_start = cls._get_year_start_date()
        cycle_dates = cls._calculate_cycle_dates(year_start)

        return cycle_dates.get(cycle_name, {"start": date.today(), "end": date.today()})


class EpicMonitorService:
    """Service for monitoring JIRA epics and identifying problems."""

    _logger = LogManager.get_instance().get_logger("EpicMonitorService")

    def __init__(self):
        """Initialize the service with JIRA assistant."""
        self.jira_assistant = JiraAssistant()

    def get_catalog_epics(self) -> List[EpicIssue]:
        """
        Fetch epics for Catalog team that are not done and in current cycle.
        JQL: type = Epic and statusCategory != Done and "Squad[Dropdown]" = "Catalog"
        Order by priority
        """
        try:
            jql_query = (
                "type = Epic AND statusCategory != Done AND "
                '"Squad[Dropdown]" = "Catalog" ORDER BY priority'
            )

            self._logger.info("Fetching Catalog epics from JIRA")
            epic_data = self.jira_assistant.fetch_issues(
                jql_query,
                fields="key,summary,status,customfield_10015,duedate,fixVersions,priority,assignee",
            )

            epics = []
            for data in epic_data:
                epic = EpicIssue(data)
                # Filter by current cycle
                if CycleDetector.is_fix_version_current_cycle(epic.fix_version):
                    epics.append(epic)

            self._logger.info(f"Found {len(epics)} epics for current cycle")
            return epics

        except Exception as e:
            self._logger.error(f"Error fetching epics: {e}", exc_info=True)
            raise JiraManagerError("Failed to fetch Catalog epics", error=str(e))

    def analyze_epic_problems(self, epics: List[EpicIssue]) -> List[EpicIssue]:
        """
        Analyze epics for problems based on the defined rules.
        """
        problematic_epics = []
        today = date.today()

        # Get configurable thresholds from environment
        business_days_threshold = int(os.getenv("EPIC_MONITOR_BUSINESS_DAYS_THRESHOLD", "3"))
        due_date_warning_days = int(os.getenv("EPIC_MONITOR_DUE_DATE_WARNING_DAYS", "3"))

        for epic in epics:
            epic.problems = []

            # Rule 1: Status is "7 PI Started" and Start Date is missing
            if epic.status == "7 PI Started" and not epic.start_date:
                epic.problems.append("Status is '7 PI Started' but Start Date is missing")

            # Rule 2: Status is "7 PI Started" and Due Date is missing
            if epic.status == "7 PI Started" and not epic.due_date:
                epic.problems.append("Status is '7 PI Started' but Due Date is missing")

            # Rule 3: Status is "7 PI Started", Start Date exists,
            # but Due Date missing for X business days
            if epic.status == "7 PI Started" and epic.start_date and not epic.due_date:
                business_days_since_start = self._calculate_business_days(epic.start_date, today)
                if business_days_since_start >= business_days_threshold:
                    epic.problems.append(
                        f"Status is '7 PI Started', started {business_days_since_start} "
                        f"business days ago, but Due Date is still missing"
                    )

            # Rule 4: Epic is overdue (Due Date in the past)
            if epic.due_date and epic.due_date < today:
                days_overdue = (today - epic.due_date).days
                epic.problems.append(f"Epic is overdue by {days_overdue} days")

            # Rule 5: Epic is approaching due date (3 or fewer business days remaining)
            if epic.due_date and epic.due_date >= today:
                business_days_remaining = self._calculate_business_days(today, epic.due_date)
                if business_days_remaining <= due_date_warning_days:
                    epic.problems.append(
                        f"Epic is approaching due date ({business_days_remaining} "
                        f"business days remaining)"
                    )

            # Rule 6: Epic is started but has no assignee
            if epic.status == "7 PI Started" and not epic.assignee_id:
                epic.problems.append(
                    "Status is '7 PI Started' but no person is assigned to this epic"
                )

            if epic.problems:
                problematic_epics.append(epic)

        return problematic_epics

    def _calculate_business_days(self, start_date: date, end_date: date) -> int:
        """Calculate business days between two dates (excluding weekends)."""
        if start_date > end_date:
            return 0

        business_days = 0
        current_date = start_date

        while current_date <= end_date:
            # Monday = 0, Sunday = 6
            if current_date.weekday() < 5:  # Monday to Friday
                business_days += 1
            current_date += timedelta(days=1)

        return business_days


class SlackNotificationService:
    """Service for sending Slack notifications."""

    _logger = LogManager.get_instance().get_logger("SlackNotificationService")

    def __init__(self, webhook_url: str):
        """Initialize with Slack webhook URL."""
        self.webhook_url = webhook_url

    def send_epic_problems_notification(
        self,
        problematic_epics: List[EpicIssue],
        slack_token: Optional[str] = None,
        recipient_id: Optional[str] = None,
    ) -> bool:
        """
        Send a Slack notification about problematic epics using Block Kit.
        Works for channels or direct messages.
        """
        if not problematic_epics:
            self._logger.info("No problematic epics to report")
            return True

        # Use environment variables if not provided
        token = slack_token or os.getenv("SLACK_BOT_TOKEN")
        channel = recipient_id or os.getenv("SLACK_CHANNEL_ID")

        if not token:
            self._logger.error("Slack token not provided and SLACK_BOT_TOKEN not set")
            return False

        if not channel:
            self._logger.error("Slack channel not provided and SLACK_CHANNEL_ID not set")
            return False

        blocks = self._format_epic_problems_blocks(problematic_epics)

        try:
            payload = {
                "channel": channel,
                "blocks": blocks,
                "text": f"{len(problematic_epics)} problematic epics detected.",
            }

            headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

            response = requests.post(
                "https://slack.com/api/chat.postMessage",
                json=payload,
                headers=headers,
                timeout=30,
            )

            if response.status_code == 200 and response.json().get("ok"):
                self._logger.info(
                    f"Successfully sent notification for {len(problematic_epics)} problematic epics"
                )
                return True
            else:
                self._logger.error(f"Failed to send Slack notification. Response: {response.text}")
                return False

        except Exception as e:
            self._logger.error(f"Error sending Slack notification: {e}", exc_info=True)
            return False

    def _format_epic_problems_blocks(
        self, problematic_epics: List[EpicIssue]
    ) -> List[Dict[str, Any]]:
        current_cycle = CycleDetector.get_current_cycle()
        blocks = []

        # Header
        blocks.append(
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"🚨 Epic Issues Alert - {current_cycle}"},
            }
        )

        # Brief intro
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{len(problematic_epics)}* epics need attention in *Catalog squad*:",
                },
            }
        )

        blocks.append({"type": "divider"})

        for epic in problematic_epics:
            jira_url = os.getenv("JIRA_URL")

            # Create epic text with conditional URL formatting
            if jira_url:
                # Create epic URL when base URL is available
                epic_url = f"{jira_url}/browse/{epic.key}"
                epic_text = (
                    f"📌 *<{epic_url}|{epic.key}>*: {epic.summary}\n" f"• Status: *{epic.status}*\n"
                )
            else:
                # Just show the key without link when base URL is not available
                epic_text = f"📌 *{epic.key}*: {epic.summary}\n" f"• Status: *{epic.status}*\n"

            if epic.assignee_slack_id:
                epic_text += f"• Assignee: <@{epic.assignee_slack_id}>\n"
            elif epic.assignee_name:
                epic_text += f"• Assignee: {epic.assignee_name} (Slack unmapped)\n"
            else:
                epic_text += "• Assignee: Unassigned\n"

            if epic.due_date:
                epic_text += f"• Due: *{epic.due_date}*\n"

            epic_text += "⚠️ *Issues:*"
            for problem in epic.problems:
                epic_text += f"\n> • {problem}"

            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": epic_text}})

            blocks.append({"type": "divider"})

        # Context with timestamp (brief)
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"🕒 Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                    }
                ],
            }
        )

        return blocks


class EpicCronService:
    """Main service that orchestrates epic monitoring and notifications."""

    _logger = LogManager.get_instance().get_logger("EpicCronService")

    def __init__(self, slack_webhook_url: Optional[str] = None):
        """Initialize the service with dependencies."""
        self.epic_monitor = EpicMonitorService()
        # Use provided webhook or environment variable
        webhook_url = slack_webhook_url or os.getenv("SLACK_WEBHOOK_URL")
        if not webhook_url:
            raise ValueError("Slack webhook URL must be provided or set in SLACK_WEBHOOK_URL")
        self.slack_service = SlackNotificationService(webhook_url)

    def run_epic_check(self) -> bool:
        """
        Main method to run the epic check process.

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self._logger.info("Starting epic monitoring check")

            # 1. Get epics for current cycle
            epics = self.epic_monitor.get_catalog_epics()

            # 2. Analyze for problems
            problematic_epics = self.epic_monitor.analyze_epic_problems(epics)

            # 3. Send notifications if problems found
            success = self.slack_service.send_epic_problems_notification(problematic_epics)

            if success:
                self._logger.info("Epic monitoring check completed successfully")
            else:
                self._logger.error("Epic monitoring check completed with errors")

            return success

        except Exception as e:
            self._logger.error(f"Error during epic monitoring check: {e}", exc_info=True)
            return False
