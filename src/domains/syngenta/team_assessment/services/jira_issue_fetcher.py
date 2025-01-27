from typing import Dict, List, Optional
from datetime import date, datetime
from utils.jira.jira_assistant import JiraAssistant
from utils.logging.logging_manager import LogManager
from utils.jira.error import JiraQueryError, JiraManagerError
import pandas as pd


class Issue:
    def __init__(
        self,
        key: str,
        summary: str,
        created_date: date,
        closed_date: Optional[date] = None,
        status: str = None,
    ):
        self.key = key
        self.summary = summary
        self.created_date = created_date
        self.closed_date = closed_date
        self.status = status

    def __repr__(self):
        return (
            f"Issue(key={self.key}, summary={self.summary}, created_date={self.created_date}, "
            f"closed_date={self.closed_date}, status={self.status})"
        )


class JiraIssueFetcher:
    """
    Fetcher class for handling Jira issue-related operations.
    """

    _logger = LogManager.get_instance().get_logger("JiraIssueFetcher")

    def __init__(self):
        """
        Initializes the JiraIssueFetcher with a Jira client.
        """
        self.jira_assistant = JiraAssistant()

    def get_bugs_created_within_dates(
        self, project_name: str, team_name: str, start_date: date, end_date: date
    ) -> List[Issue]:
        """
        Fetches bugs created within a given date range and analyzes their status changes.

        Args:
            project_name (str): The Jira project key.
            team_name (str): The name of the team.
            start_date (date): The start date of the range.
            end_date (date): The end date of the range.

        Returns:
            List[Issue]: A list of bug issues with their created and closed dates.
        """
        try:
            if not isinstance(start_date, date) or not isinstance(end_date, date):
                raise ValueError("Start and end dates must be valid date objects.")

            bugs = self._fetch_bugs(project_name, team_name, start_date, end_date)

            bugs_list = []
            for bug in bugs:
                changelog = bug.get("changelog", {}).get("histories", [])
                closed_date = self._analyze_issue_changelog(changelog)
                created_date = datetime.strptime(
                    bug["fields"]["created"], "%Y-%m-%dT%H:%M:%S.%f%z"
                ).date()
                if closed_date and closed_date <= pd.Timestamp(end_date):
                    bugs_list.append(
                        Issue(
                            key=bug["key"],
                            summary=bug["fields"]["summary"],
                            created_date=pd.Timestamp(created_date),
                            closed_date=pd.Timestamp(closed_date),
                            status=bug["fields"]["status"]["name"],
                        )
                    )

            return bugs_list
        except JiraQueryError as e:
            self._logger.error(f"Error fetching bugs: {e}", exc_info=True)
            raise JiraManagerError("Failed to fetch bugs.", error=str(e))
        except Exception as e:
            self._logger.error(f"Unexpected error: {e}", exc_info=True)
            raise JiraManagerError("An unexpected error occurred.", error=str(e))

    def get_epics_by_keys(self, epic_keys: List[str]) -> List[Issue]:
        """
        Fetches epics by their keys and analyzes their status changes.

        Args:
            epic_keys (List[str]): The list of epic keys.

        Returns:
            List[Issue]: A list of epic issues with their created and closed dates.
        """
        try:
            epics = self._fetch_epic_by_keys(epic_keys)

            epics_list = []
            for epic in epics:
                changelog = epic.get("changelog", {}).get("histories", [])
                closed_date = self._analyze_issue_changelog(changelog)
                created_date = datetime.strptime(
                    epic["fields"]["created"], "%Y-%m-%dT%H:%M:%S.%f%z"
                ).date()
                epics_list.append(
                    Issue(
                        key=epic["key"],
                        summary=epic["fields"]["summary"],
                        created_date=pd.Timestamp(created_date),
                        closed_date=pd.Timestamp(closed_date),
                        status=epic["fields"]["status"]["name"],
                    )
                )

            return epics_list
        except JiraQueryError as e:
            self._logger.error(f"Error fetching epics: {e}", exc_info=True)
            raise JiraManagerError("Failed to fetch epics.", error=str(e))
        except Exception as e:
            self._logger.error(f"Unexpected error: {e}", exc_info=True)
            raise JiraManagerError("An unexpected error occurred.", error=str(e))

    def get_epics_closed_during_period(
        self, project_name: str, team_name: str, start_date: date, end_date: date
    ) -> List[Issue]:
        """
        Fetches all epics for the DA Backbone squad within a specific date range.

        Returns:
            List[Issue]: A list of epic issues with their created and closed dates.
        """

        try:
            epics = self._fetch_epics_closed_by_period(
                project_name, team_name, start_date, end_date
            )
            epics_list = []
            for epic in epics:
                changelog = epic.get("changelog", {}).get("histories", [])
                closed_date = self._analyze_issue_changelog(changelog)
                created_date = datetime.strptime(
                    epic["fields"]["created"], "%Y-%m-%dT%H:%M:%S.%f%z"
                ).date()
                epics_list.append(
                    Issue(
                        key=epic["key"],
                        summary=epic["fields"]["summary"],
                        created_date=pd.Timestamp(created_date),
                        closed_date=pd.Timestamp(closed_date),
                        status=epic["fields"]["status"]["name"],
                    )
                )

            return epics_list
        except JiraQueryError as e:
            self._logger.error(f"Error fetching all epics: {e}", exc_info=True)
            raise JiraManagerError("Failed to fetch all epics.", error=str(e))
        except Exception as e:
            self._logger.error(f"Unexpected error: {e}", exc_info=True)
            raise JiraManagerError("An unexpected error occurred.", error=str(e))

    def _fetch_bugs(
        self, project_name: str, team_name: str, start_date: date, end_date: date
    ) -> List[Dict]:
        """
        Fetches bug issues created within a given date range for a specific team.

        Args:
            project_name (str): The Jira project key.
            team_name (str): The name of the team.
            start_date (date): The start date of the range.
            end_date (date): The end date of the range.

        Returns:
            List[Dict]: A list of bug issues.
        """
        jql_query = (
            f"project = '{project_name}' AND issuetype = Bug AND 'Squad[Dropdown]' = '{team_name}' "
            f"AND parent is EMPTY AND (status changed to ('10 Done', '11 Archived', 'Done') "
            f"during ('{start_date.strftime('%Y-%m-%d')}', '{end_date.strftime('%Y-%m-%d')}') "
            f"OR created >= '{start_date.strftime('%Y-%m-%d')}' "
            f"AND created <= '{end_date.strftime('%Y-%m-%d')}') ORDER BY created DESC"
        )
        self._logger.info(
            f"Fetching bugs for project '{project_name}' and team '{team_name}' "
            f"created between '{start_date}' and '{end_date}'."
        )
        try:
            return self.jira_assistant.fetch_issues(
                jql_query,
                fields="key,summary,created,status,changelog",
                expand_changelog=True,
            )
        except JiraQueryError as e:
            raise JiraQueryError("Error fetching bugs", jql=jql_query, error=str(e))

    def _fetch_epic_by_keys(self, epic_keys: List[str]) -> List[Dict]:
        """
        Fetches epic issues by their keys.

        Args:
            epic_keys (List[str]): The list of epic keys.

        Returns:
            List[Dict]: A list of epic issues.
        """
        jql_query = (
            f"key in ({','.join([f'\"{key}\"' for key in epic_keys])}) "
            f"AND statusCategory = Done AND status not in ('11 Archived', 'Archive', 'Archived')"
        )
        self._logger.info(f"Fetching epics with keys: {', '.join(epic_keys)}")
        try:
            return self.jira_assistant.fetch_issues(
                jql_query,
                fields="key,summary,created,status,changelog",
                expand_changelog=True,
            )
        except JiraQueryError as e:
            raise JiraQueryError("Error fetching epics by keys", jql=jql_query, error=str(e))

    def _fetch_epics_closed_by_period(
        self, project_name: str, team_name: str, start_date: date, end_date: date
    ) -> List[Dict]:
        """
        Fetches epics closed within a specified date range for a given project and team.
        Args:
            project_name (str): The name of the Jira project.
            team_name (str): The name of the team (Squad) in the Jira project.
            start_date (date): The start date of the period to fetch closed epics.
            end_date (date): The end date of the period to fetch closed epics.
        Returns:
            List[Dict]: A list of dictionaries containing details of the closed epics.
        Raises:
            JiraQueryError: If there is an error fetching the closed epics from Jira.
        """
        jql_query = (
            f"project = '{project_name}' AND \"Squad[Dropdown]\" = '{team_name}' "
            f"AND issuetype = Epic "
            f"AND status IN (Done, '10 Done') AND status CHANGED TO (Done, '10 Done') "
            f"AFTER '{start_date.strftime('%Y-%m-%d')}' AND status CHANGED TO "
            f"(Done, '10 Done') BEFORE '{end_date.strftime('%Y-%m-%d')}' ORDER BY created DESC"
        )
        self._logger.info(
            "Fetching all epics for DA Backbone squad within the specified date range."
        )
        try:
            return self.jira_assistant.fetch_issues(
                jql_query,
                fields="key,summary,created,status,changelog",
                expand_changelog=True,
            )
        except JiraQueryError as e:
            raise JiraQueryError("Error fetching closed epics", jql=jql_query, error=str(e))

    def _analyze_issue_changelog(self, changelog: List[Dict]) -> Optional[date]:
        """
        Analyzes the changelog to find the last time the issue was moved to Done.

        Args:
            changelog (List[Dict]): The changelog histories of the issue.

        Returns:
            Optional[date]: The date the issue was moved to Done, or None if not found.
        """
        closed_date = None
        for history in changelog:
            for item in history.get("items", []):
                if item.get("field") == "status" and item.get("toString") in ["Done", "10 Done"]:
                    closed_date = datetime.strptime(
                        history["created"], "%Y-%m-%dT%H:%M:%S.%f%z"
                    ).date()

        return pd.Timestamp(closed_date) if closed_date else None
