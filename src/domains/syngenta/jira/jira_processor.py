from typing import Dict, List, Optional, Tuple
from utils.jira.jira_assistant import JiraAssistant
from utils.logging.logging_manager import LogManager
from utils.jira.error import JiraQueryError, JiraManagerError
from datetime import date, datetime


class JiraProcessor:
    """
    Processor class for handling Jira-related operations.
    """

    _logger = LogManager.get_instance().get_logger("JiraProcessor")

    def __init__(self):
        """
        Initializes the JiraProcessor with a Jira client.

        Args:
            jira_client: The Jira client to use for API interactions.
        """
        self.jira_assistant = JiraAssistant()

    def fill_missing_dates_for_completed_epics(
        self,
        project: str,
        team_name: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ):
        """
        Fills missing dates for completed epics in Jira.

        Args:
            project (str): Jira project key.
            team_name (str): Team name associated with the epics.
            start_date (Optional[date]): Start date for filtering epics (optional).
            end_date (Optional[date]): End date for filtering epics (optional).

        Raises:
            ValueError: If required parameters are missing or invalid.
            JiraManagerError: For API-related issues.
        """
        try:
            if start_date and end_date:
                if start_date and not isinstance(start_date, date):
                    raise ValueError("Start date must be a valid date object.")
                if end_date and not isinstance(end_date, date):
                    raise ValueError("End date must be a valid date object.")
                self._update_epic_dates(project, team_name, start_date, end_date)
            else:
                if not project or not team_name:
                    raise ValueError("Project and team name must be provided.")
                self._update_epic_dates_with_changelog(project, team_name)

        except JiraManagerError as e:
            self._logger.error(f"Error processing Jira epics: {e}", exc_info=True)
            raise

    def _get_epics(self, project_name: str, team_name: str) -> List[Dict]:
        """
        Fetches epics from Jira with missing dates.

        Args:
            project_name (str): Jira project key.
            team_name (str): Team name.

        Returns:
            List[Dict]: List of epics.
        """
        jql_query = (
            f"project = '{project_name}' AND type = Epic AND status = Done "
            f"AND 'Squad[Dropdown]' = '{team_name}' AND ("
            f"'Start date' is EMPTY OR 'End date' is EMPTY)"
        )
        self._logger.info(f"Fetching epics for project '{project_name}', team '{team_name}'.")
        try:
            return self.jira_assistant.fetch_issues(
                jql_query, fields="key,summary", expand_changelog=True
            )
        except JiraQueryError as e:
            raise JiraQueryError("Error fetching epics", jql=jql_query, error=str(e))

    def _analyze_changelog(self, changelog: List[Dict]) -> Tuple[Optional[date], Optional[date]]:
        """
        Analyzes changelog to extract inferred start and end dates.

        Args:
            changelog (List[Dict]): The changelog histories.

        Returns:
            Tuple[Optional[date], Optional[date]]: Start and end dates.
        """
        start_date = None
        end_date = None
        for history in changelog:
            for item in history.get("items", []):
                if item.get("toString") == "7 PI Started" and not start_date:
                    start_date = datetime.strptime(
                        history["created"], "%Y-%m-%dT%H:%M:%S.%f%z"
                    ).date()
                if (
                    item.get("fromString") == "7 PI Started"
                    and item.get("toString") == "Done"
                    and not end_date
                ):
                    end_date = datetime.strptime(
                        history["created"], "%Y-%m-%dT%H:%M:%S.%f%z"
                    ).date()
                if start_date and end_date:
                    break
        return start_date, end_date

    def _update_epic_dates_with_changelog(self, project: str, team_name: str):
        """
        Updates epic dates by analyzing their changelog.

        Args:
            project (str): Jira project key.
            team_name (str): Team name associated with the epics.

        Raises:
            JiraManagerError: For issues during processing.
        """
        epics = self._get_epics(project, team_name)
        for epic in epics:
            issue_key = epic["key"]
            changelog = epic.get("changelog", {}).get("histories", [])
            start_date, end_date = self._analyze_changelog(changelog)
            if start_date or end_date:
                self._update_epic_dates(issue_key, start_date=start_date, end_date=end_date)

    def _update_epic_dates(
        self, issue_key: str, start_date: Optional[date] = None, end_date: Optional[date] = None
    ):
        """
        Updates the dates of a specific epic.

        Args:
            issue_key (str): Key of the Jira issue.
            start_date (Optional[date]): Start date to update.
            end_date (Optional[date]): End date to update.
        """
        payload = {
            "fields": {
                "customfield_10015": start_date.isoformat() if start_date else None,
                "customfield_10233": end_date.isoformat() if end_date else None,
            }
        }
        try:
            self.jira_assistant.client.put(f"issue/{issue_key}", payload)
            self._logger.info(f"Updated epic '{issue_key}' with dates: {payload}")
        except Exception as e:
            raise JiraManagerError(
                f"Failed to update epic '{issue_key}'", payload=payload, error=str(e)
            )

    def fetch_custom_fields(self) -> List[Dict]:
        """
        Fetches a list of all custom fields available in Jira.

        Returns:
            List[Dict]: List of custom fields with their details.

        Raises:
            JiraManagerError: If fetching custom fields fails.
        """
        try:
            self._logger.info("Fetching custom fields using the /field/search endpoint.")
            response = self.jira_assistant.client.get("field/search", params={"type": "custom"})
            response.raise_for_status()
            fields_data = response.json()

            # Extract and format the custom fields
            custom_fields = []
            for field in fields_data.get("values", []):
                custom_fields.append(
                    {
                        "id": field["id"],
                        "name": field["name"],
                        "type": field["schema"].get("type"),
                        "custom": field["schema"].get("custom"),
                        "description": field.get("description", "No description provided"),
                    }
                )

            return custom_fields
        except Exception as e:
            self._logger.error(f"Failed to fetch custom fields: {e}")
            raise JiraManagerError("Error fetching custom fields.", error=str(e))
