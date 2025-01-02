from typing import Dict, List, Optional

from utils.error_manager import handle_generic_exception
from utils.logging_manager import LogManager
from utils.cache_manager import CacheManager


class JiraAssistant:
    """
    A generic assistant for interacting with Jira APIs.
    Includes core methods for fetching, creating, and updating Jira data.
    Designed to simplify and streamline Jira API consumption.
    """

    _logger = LogManager.get_instance().get_logger("JiraAssistant")
    _cache_manager = CacheManager.get_instance()

    def __init__(self, cache_expiration: int = 60):
        """
        Initializes the JiraAssistant with specified parameters.

        Args:
            client_type (str): The type of Jira client to use.
            cache_expiration (int): Cache expiration time in minutes.
        """
        self.cache_manager = CacheManager()
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
        return f"{prefix}_{hash(frozenset(kwargs.items()))}.json"

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

            cache_key = self._generate_cache_key(
                "issues", jql=jql_query, fields=fields, max_results=max_results
            )
            cached_data = self.cache_manager.load_from_cache(
                cache_key, expiration_minutes=self.cache_expiration
            )
            if cached_data:
                self._logger.info(f"Loaded issues from cache for JQL: {jql_query}")
                return cached_data

            self._logger.info(f"Fetching issues with JQL: {jql_query}")
            response = self.client.get(
                "search", params={"jql": jql_query, "fields": fields, "maxResults": max_results}
            )

            if response:
                self.cache_manager.save_to_cache(cache_key, response)

            return response
        except Exception as e:
            handle_generic_exception(e, f"Failed to fetch issues with JQL '{jql_query}'")
            return []

    def update_issue(self, issue_key: str, fields: Dict) -> Optional[Dict]:
        """
        Update fields of a Jira issue.

        Args:
            issue_key (str): The key of the issue to update.
            fields (Dict): A dictionary of fields to update.

        Returns:
            Optional[Dict]: Response from the Jira API or None if failed.
        """
        try:
            self._logger.info(f"Updating issue {issue_key} with fields: {fields}")
            return self.client.put(f"issue/{issue_key}", {"fields": fields})
        except Exception as e:
            handle_generic_exception(e, f"Failed to update issue {issue_key}")
            return None

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
            cached_data = self.cache_manager.load_from_cache(
                cache_key, expiration_minutes=self.cache_expiration
            )
            if cached_data:
                self._logger.info(f"Loaded components from cache for project '{project_key}'.")
                return cached_data

            self._logger.info(f"Fetching components for project '{project_key}'")
            response = self.client.get(f"project/{project_key}/components")

            if response:
                self.cache_manager.save_to_cache(cache_key, response)

            return response
        except Exception as e:
            handle_generic_exception(e, f"Failed to fetch components for project '{project_key}'")
            return []

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
            if response:
                self._logger.info(f"Issue created with key: {response.get('key')}")
            return response
        except Exception as e:
            handle_generic_exception(e, "Failed to create issue")
            return None

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
            cached_data = self.cache_manager.load_from_cache(
                cache_key, expiration_minutes=self.cache_expiration
            )
            if cached_data:
                self._logger.info(
                    f"Loaded metadata from cache for issue type '{issue_type_id}' "
                    f"in project '{project_key}'."
                )
                return cached_data

            self._logger.info(
                f"Fetching metadata for issue type '{issue_type_id}' in project '{project_key}'"
            )
            response = self.client.get(f"issue/createmeta/{project_key}/issuetypes/{issue_type_id}")

            if response:
                self.cache_manager.save_to_cache(cache_key, response)

            return response
        except Exception as e:
            handle_generic_exception(
                e,
                f"Failed to fetch metadata for issue type '{issue_type_id}' "
                f"in project '{project_key}'",
            )
            return None

    def create_bulk_issues(self, project_key: str, issues_data: List[Dict]) -> Optional[Dict]:
        """
        Create multiple Jira issues in bulk.

        Args:
            project_key (str): The project key.
            issues_data (List[Dict]): A list of issue data payloads.

        Returns:
            Optional[Dict]: Response from Jira API or None if failed.
        """
        try:
            self._logger.info(f"Creating bulk issues in project '{project_key}'")
            response = self.client.post("issue/bulk", {"issueUpdates": issues_data})

            if response:
                self._logger.info(f"Bulk issues created successfully in project '{project_key}'")

            return response
        except Exception as e:
            handle_generic_exception(e, f"Failed to create bulk issues in project '{project_key}'")
            return None
