from datetime import datetime
from typing import Dict, List, Optional
from utils.logging.logging_manager import LogManager
from utils.cache_manager.cache_manager import CacheManager
from utils.error.jira_assistant_errors import (
    JiraQueryError,
    JiraIssueCreationError,
    JiraComponentFetchError,
    JiraMetadataFetchError,
)


class JiraAssistant:
    """
    A generic assistant for interacting with Jira APIs.
    Includes core methods for fetching, creating, and updating Jira data.
    Designed to simplify and streamline Jira API consumption.
    """

    _logger = LogManager.get_instance().get_logger("JiraAssistant")

    def __init__(self, client, cache_expiration: int = 60):
        """
        Initializes the JiraAssistant with specified parameters.

        Args:
            client: The Jira client to use for API interactions.
            cache_expiration (int): Cache expiration time in minutes.
        """
        self.client = client
        self.cache_manager = CacheManager.get_instance()
        self.cache_expiration = cache_expiration

    def _generate_cache_key(self, prefix: str, **kwargs) -> str:
        """
        Generates a cache key based on a prefix and additional parameters.

        Args:
            prefix (str): The base prefix for the cache key.
            **kwargs: Additional parameters to include in the cache key.

        Returns:
            str: The generated cache key.
        """
        return f"{prefix}_{hash(frozenset(kwargs.items()))}"

    def _load_from_cache(self, cache_key: str) -> Optional[Dict]:
        """
        Loads data from the cache if available and valid.

        Args:
            cache_key (str): The cache key to retrieve data for.

        Returns:
            Optional[Dict]: Cached data if available, None otherwise.
        """
        try:
            return self.cache_manager.load(cache_key, expiration_minutes=self.cache_expiration)
        except Exception as e:
            self._logger.warning(f"Cache miss or load failure for key '{cache_key}': {e}")
            return None

    def _save_to_cache(self, cache_key: str, data: Dict):
        """
        Saves data to the cache.

        Args:
            cache_key (str): The cache key to store data for.
            data (Dict): The data to be cached.
        """
        try:
            self.cache_manager.save(cache_key, data)
            self._logger.info(f"Data cached under key: {cache_key}")
        except Exception as e:
            self._logger.error(f"Failed to cache data for key '{cache_key}': {e}")

    def fetch_issues(
        self,
        jql_query: str,
        fields: str = "*",
        max_results: int = 100,
        expand_changelog: bool = False,
    ) -> List[Dict]:
        """
        Fetch issues from Jira using a JQL query.

        Args:
            jql_query (str): The JQL query to execute.
            fields (str): Fields to include in the response.
            max_results (int): Maximum number of results to fetch.
            expand_changelog (bool): Whether to include changelog data.

        Returns:
            List[Dict]: A list of issues.
        """
        try:
            if expand_changelog:
                fields += ",changelog"

            issues = []
            start_at = 0

            while True:
                cache_key = self._generate_cache_key(
                    "issues",
                    jql=jql_query,
                    fields=fields,
                    start_at=start_at,
                    max_results=max_results,
                )
                cached_data = self._load_from_cache(cache_key)
                if cached_data:
                    self._logger.info(
                        f"Loaded issues from cache for JQL: {jql_query} (start_at={start_at})"
                    )
                    issues.extend(cached_data.get("issues", []))
                    if len(issues) >= cached_data.get("total", 0):
                        break
                    start_at += max_results
                    continue

                self._logger.info(f"Fetching issues with JQL: {jql_query} (start_at={start_at})")
                response = self.client.get(
                    "search",
                    params={
                        "jql": jql_query,
                        "fields": fields,
                        "startAt": start_at,
                        "maxResults": max_results,
                    },
                )

                if not response:
                    raise JiraQueryError("No response received from Jira API.", jql=jql_query)

                self._save_to_cache(cache_key, response)
                issues.extend(response.get("issues", []))

                if len(issues) >= response.get("total", 0):
                    break
                start_at += max_results

            return issues
        except JiraQueryError as e:
            self._logger.error(e)
            raise
        except Exception as e:
            raise JiraQueryError("Error fetching issues.", jql=jql_query, error=str(e)) from e

    def fetch_project_components(self, project_key: str) -> List[Dict]:
        """
        Fetch all components for a specific Jira project.

        Args:
            project_key (str): The key of the Jira project.

        Returns:
            List[Dict]: A list of project components.
        """
        try:
            cache_key = self._generate_cache_key("project_components", project_key=project_key)
            cached_data = self._load_from_cache(cache_key)
            if cached_data:
                self._logger.info(f"Loaded components from cache for project '{project_key}'.")
                return cached_data

            self._logger.info(f"Fetching components for project '{project_key}'")
            response = self.client.get(f"project/{project_key}/components")

            if not response:
                raise JiraComponentFetchError(
                    "No response received for project components.", project_key=project_key
                )

            self._save_to_cache(cache_key, response)

            return response
        except JiraComponentFetchError as e:
            self._logger.error(e)
            raise
        except Exception as e:
            raise JiraComponentFetchError(
                "Error fetching project components.", project_key=project_key, error=str(e)
            ) from e

    def create_issue(self, project_key: str, payload: Dict) -> Optional[Dict]:
        """
        Create a Jira issue.

        Args:
            project_key (str): The project key.
            payload (Dict): The issue data payload.

        Returns:
            Optional[Dict]: The created issue data or None if failed.
        """
        try:
            self._logger.info(f"Creating issue in project '{project_key}' with payload: {payload}")
            response = self.client.post("issue", payload)
            if not response:
                raise JiraIssueCreationError(
                    "Failed to create issue.", project_key=project_key, payload=payload
                )

            self._logger.info(f"Issue created with key: {response.get('key')}")
            return response
        except JiraIssueCreationError as e:
            self._logger.error(e)
            raise
        except Exception as e:
            raise JiraIssueCreationError(
                "Error creating issue.", project_key=project_key, payload=payload, error=str(e)
            ) from e

    def fetch_metadata(self, project_key: str, issue_type_id: str) -> Optional[Dict]:
        """
        Fetch metadata for a specific issue type in a Jira project.

        Args:
            project_key (str): The project key.
            issue_type_id (str): The ID of the issue type.

        Returns:
            Optional[Dict]: Metadata of the specified issue type.
        """
        try:
            cache_key = self._generate_cache_key(
                "metadata", project_key=project_key, issue_type_id=issue_type_id
            )
            cached_data = self._load_from_cache(cache_key)
            if cached_data:
                self._logger.info(
                    f"Loaded metadata from cache for issue type '{issue_type_id}' "
                    "in project '{project_key}'."
                )
                return cached_data

            self._logger.info(
                f"Fetching metadata for issue type '{issue_type_id}' in project '{project_key}'"
            )
            response = self.client.get(f"issue/createmeta/{project_key}/issuetypes/{issue_type_id}")

            if not response:
                raise JiraMetadataFetchError(
                    "No response received for metadata fetch.",
                    project_key=project_key,
                    issue_type_id=issue_type_id,
                )

            self._save_to_cache(cache_key, response)

            return response
        except JiraMetadataFetchError as e:
            self._logger.error(e)
            raise
        except Exception as e:
            raise JiraMetadataFetchError(
                "Error fetching metadata.",
                project_key=project_key,
                issue_type_id=issue_type_id,
                error=str(e),
            ) from e

    def fetch_completed_epics(self, team_name: str, time_period_days: int) -> List[Dict]:
        """
        Fetch completed epics for a specific team within a given time period.

        Args:
            team_name (str): The name of the team to filter epics.
            time_period_days (int): Number of days in the past to search.

        Returns:
            List[Dict]: A list of completed epics.
        """
        try:
            cache_key = self._generate_cache_key(
                "completed_epics", team_name=team_name, time_period_days=time_period_days
            )
            cached_data = self._load_from_cache(cache_key)
            if cached_data:
                self._logger.info(f"Loaded completed epics from cache for team '{team_name}'.")
                return cached_data

            time_period_ago = datetime.datetime.now() - datetime.timedelta(days=time_period_days)
            jql_query = (
                f"project = 'Cropwise Core Services' AND type = Epic "
                f"AND 'Squad[Dropdown]' = '{team_name}' "
                f"AND statusCategory = Done AND resolved >= {time_period_ago.strftime('%Y-%m-%d')}"
            )

            self._logger.info(
                f"Fetching completed epics for team '{team_name}' within the "
                "last {time_period_days} days."
            )
            epics = self.fetch_issues(jql_query)

            if epics:
                self._save_to_cache(cache_key, epics)

            return epics
        except Exception as e:
            raise JiraQueryError(
                f"Failed to fetch completed epics for team '{team_name}' within "
                "the last {time_period_days} days.",
                error=str(e),
            ) from e

    def fetch_open_issues_by_type(
        self, team_name: str, issue_type: str = "Epic", fix_version: Optional[str] = None
    ) -> List[Dict]:
        """
        Fetch open issues of a specified type for a team, optionally filtered by fix version.

        Args:
            team_name (str): The name of the team to filter issues.
            issue_type (str): Type of issue to filter (e.g., Epic, Story, Task).
            fix_version (Optional[str]): Fix version to filter issues (optional).

        Returns:
            List[Dict]: A list of open issues.
        """
        try:
            cache_key = self._generate_cache_key(
                "open_issues", team_name=team_name, issue_type=issue_type, fix_version=fix_version
            )
            cached_data = self._load_from_cache(cache_key)
            if cached_data:
                self._logger.info(f"Loaded open {issue_type}s from cache for team '{team_name}'.")
                return cached_data

            jql_query = (
                f"project = 'Cropwise Core Services' AND type = '{issue_type}' "
                f"AND 'Squad[Dropdown]' = '{team_name}' AND statusCategory != Done"
            )
            if fix_version:
                jql_query += f" AND fixVersion = '{fix_version}'"

            self._logger.info(
                f"Fetching open {issue_type}s for team '{team_name}', fix version '{fix_version}'."
            )
            open_issues = self.fetch_issues(jql_query)

            if open_issues:
                self._save_to_cache(cache_key, open_issues)

            return open_issues
        except Exception as e:
            raise JiraQueryError(
                f"Failed to fetch open issues of type '{issue_type}' for team '{team_name}' "
                "with fix version '{fix_version}'.",
                error=str(e),
            ) from e
