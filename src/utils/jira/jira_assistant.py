from datetime import datetime
import hashlib
from typing import Dict, List, Optional
from utils.data.json_manager import JSONManager
from utils.jira.jira_api_client import JiraApiClient
from utils.logging.logging_manager import LogManager
from utils.cache_manager.cache_manager import CacheManager
from utils.jira.error import (
    JiraQueryError,
    JiraIssueCreationError,
    JiraComponentFetchError,
    JiraComponentCreationError,
    JiraComponentDeletionError,
    JiraIssueComponentUpdateError,
    JiraMetadataFetchError,
)
from utils.jira.jira_config import JiraConfig


class JiraAssistant:
    """
    A generic assistant for interacting with Jira APIs.
    Includes core methods for fetching, creating, and updating Jira data.
    Designed to simplify and streamline Jira API consumption.
    """

    _logger = LogManager.get_instance().get_logger("JiraAssistant")

    def __init__(self, cache_expiration: int = 60):
        """
        Initializes the JiraAssistant with specified parameters.

        Args:
            cache_expiration (int): Cache expiration time in minutes.
        """
        jira_config = JiraConfig()
        if not jira_config.base_url or not jira_config.email or not jira_config.api_token:
            raise ValueError(
                "JiraConfig is missing required fields: base_url, email, or api_token. "
                f"base_url={jira_config.base_url}, "
                f"email={jira_config.email}, "
                f"api_token={'***' if jira_config.api_token else None}"
            )
        self.client = JiraApiClient(jira_config.base_url, jira_config.email, jira_config.api_token)
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
        # Sort the items and convert them to a JSON string for deterministic ordering
        sorted_items = JSONManager.create_json(kwargs)
        # Create a consistent hash using SHA-256
        hash_object = hashlib.sha256(sorted_items.encode("utf-8"))
        # Convert the hash to a hexadecimal string
        hash_hex = hash_object.hexdigest()
        return f"{prefix}_{hash_hex}"

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
                if isinstance(cached_data, list):
                    return cached_data
                elif isinstance(cached_data, dict) and "components" in cached_data:
                    components = cached_data["components"]
                    if isinstance(components, list):
                        return components
                    else:
                        self._logger.warning(
                            f"Cached 'components' for project '{project_key}' is not a list. "
                            "Returning empty list."
                        )
                        return []
                else:
                    self._logger.warning(
                        f"Cached data for project '{project_key}' is not a list or dict "
                        f"with 'components'. Returning empty list."
                    )
                    return []

            self._logger.info(f"Fetching components for project '{project_key}'")
            response = self.client.get(f"project/{project_key}/components")

            if not response:
                raise JiraComponentFetchError(
                    "No response received for project components.", project_key=project_key
                )

            # Ensure response is a list for cache and return
            if isinstance(response, list):
                self._save_to_cache(cache_key, {"components": response})
                return response
            elif isinstance(response, dict) and "components" in response:
                components = response["components"]
                if isinstance(components, list):
                    self._save_to_cache(cache_key, {"components": components})
                    return components
                else:
                    self._logger.warning(
                        f"Response 'components' for project '{project_key}' is not a list. "
                        "Returning empty list."
                    )
                    self._save_to_cache(cache_key, {"components": []})
                    return []
            else:
                self._logger.warning(
                    f"Response for project '{project_key}' is not a list or dict "
                    f"with 'components'. Returning empty list."
                )
                self._save_to_cache(cache_key, {"components": []})
                return []
        except JiraComponentFetchError as e:
            self._logger.error(e)
            raise
        except Exception as e:
            raise JiraComponentFetchError(
                "Error fetching project components.", project_key=project_key, error=str(e)
            ) from e

    def create_component(
        self,
        project_key: str,
        name: str,
        description: Optional[str] = None,
        assignee_type: str = "PROJECT_DEFAULT",
        lead: Optional[str] = None,
    ) -> Optional[Dict]:
        """
        Create a component in a Jira project.

        Args:
            project_key (str): The key of the Jira project.
            name (str): The name of the component.
            description (str): Optional description for the component.
            assignee_type (str): The type of assignee (PROJECT_DEFAULT, COMPONENT_LEAD,
                PROJECT_LEAD, UNASSIGNED).
            lead (str): Optional account ID of the component lead.

        Returns:
            Optional[Dict]: The created component data or None if failed.
        """
        try:
            payload = {
                "name": name,
                "project": project_key,
                "assigneeType": assignee_type,
            }

            if description is not None:
                payload["description"] = description

            if lead is not None:
                payload["leadAccountId"] = lead

            self._logger.info(f"Creating component '{name}' in project '{project_key}'")
            response = self.client.post("component", payload)

            if not response:
                raise JiraComponentCreationError(
                    "Failed to create component.", project_key=project_key, name=name
                )

            self._logger.info(f"Component created with ID: {response.get('id')}")
            return response
        except JiraComponentCreationError as e:
            self._logger.error(e)
            raise
        except Exception as e:
            raise JiraComponentCreationError(
                "Error creating component.", project_key=project_key, name=name, error=str(e)
            ) from e

    def delete_component(self, component_id: str) -> bool:
        """
        Delete a component from Jira.

        Args:
            component_id (str): The ID of the component to delete.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            self._logger.info(f"Deleting component with ID '{component_id}'")
            self.client.delete(f"component/{component_id}")

            # DELETE requests typically return None for successful deletions
            self._logger.info(f"Component '{component_id}' deleted successfully")
            return True
        except Exception as e:
            self._logger.error(f"Error deleting component '{component_id}': {e}")
            raise JiraComponentDeletionError(
                "Error deleting component.", component_id=component_id, error=str(e)
            ) from e

    def create_components_batch(self, project_key: str, components: List[Dict]) -> List[Dict]:
        """
        Create multiple components in a Jira project.

        Args:
            project_key (str): The key of the Jira project.
            components (List[Dict]): List of component data dictionaries.

        Returns:
            List[Dict]: List of created components with their results.
        """
        results = []
        for component_data in components:
            try:
                name = str(component_data.get("name", ""))
                description = component_data.get("description")
                assignee_type = component_data.get("assignee_type", "PROJECT_DEFAULT")
                lead = component_data.get("lead")

                result = self.create_component(project_key, name, description, assignee_type, lead)
                results.append(
                    {
                        "name": name,
                        "status": "success",
                        "component": result,
                    }
                )
            except Exception as e:
                component_name = component_data.get("name")
                self._logger.error(f"Failed to create component '{component_name}': {e}")
                results.append(
                    {
                        "name": component_data.get("name"),
                        "status": "error",
                        "error": str(e),
                    }
                )
        return results

    def delete_components_batch(self, component_ids: List[str]) -> List[Dict]:
        """
        Delete multiple components from Jira.

        Args:
            component_ids (List[str]): List of component IDs to delete.

        Returns:
            List[Dict]: List of deletion results.
        """
        results = []
        for component_id in component_ids:
            try:
                success = self.delete_component(component_id)
                results.append(
                    {"component_id": component_id, "status": "success" if success else "failed"}
                )
            except Exception as e:
                self._logger.error(f"Failed to delete component '{component_id}': {e}")
                results.append(
                    {
                        "component_id": component_id,
                        "status": "error",
                        "error": str(e),
                    }
                )
        return results

    def update_issue_components(self, issue_key: str, component_id: str) -> bool:
        """
        Update an issue to replace all existing components with a single new component.

        Args:
            issue_key (str): The key of the issue to update.
            component_id (str): The ID of the component to set on the issue.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            payload = {"fields": {"components": [{"id": str(component_id)}]}}

            self._logger.info(f"Updating issue '{issue_key}' with component ID '{component_id}'")
            self.client.put(f"issue/{issue_key}", payload)

            # PUT requests typically return None for successful updates
            self._logger.info(f"Issue '{issue_key}' components updated successfully")
            return True
        except Exception as e:
            self._logger.error(f"Error updating components for issue '{issue_key}': {e}")
            raise JiraIssueComponentUpdateError(
                "Error updating issue components.",
                issue_key=issue_key,
                component_id=component_id,
                error=str(e),
            ) from e

    def update_issues_components_batch(self, issues_components: List[Dict]) -> List[Dict]:
        """
        Update multiple issues with their respective components.

        Args:
            issues_components (List[Dict]): List of dictionaries with 'key' and 'component' fields.

        Returns:
            List[Dict]: List of update results.
        """
        results = []
        for issue_data in issues_components:
            try:
                issue_key = str(issue_data.get("key", ""))
                component_id = str(issue_data.get("component", ""))

                success = self.update_issue_components(issue_key, component_id)
                results.append(
                    {
                        "issue_key": issue_key,
                        "component_id": component_id,
                        "status": "success" if success else "failed",
                    }
                )
            except Exception as e:
                issue_key = issue_data.get("key")
                component_id = issue_data.get("component")
                self._logger.error(f"Failed to update issue '{issue_key}': {e}")
                results.append(
                    {
                        "issue_key": issue_key,
                        "component_id": component_id,
                        "status": "error",
                        "error": str(e),
                    }
                )
        return results

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
                if isinstance(cached_data, list):
                    return cached_data
                elif isinstance(cached_data, dict) and "epics" in cached_data:
                    return cached_data["epics"]
                else:
                    return []

            from datetime import timedelta

            time_period_ago = datetime.now() - timedelta(days=time_period_days)
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
                self._save_to_cache(cache_key, {"epics": epics})

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
                if isinstance(cached_data, list):
                    return cached_data
                elif isinstance(cached_data, dict) and "issues" in cached_data:
                    return cached_data["issues"]
                else:
                    return []

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
                self._save_to_cache(cache_key, {"issues": open_issues})

            return open_issues
        except Exception as e:
            raise JiraQueryError(
                f"Failed to fetch open issues of type '{issue_type}' for team '{team_name}' "
                "with fix version '{fix_version}'.",
                error=str(e),
            ) from e

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
            issues = []
            start_at = 0
            expand = ["changelog"] if expand_changelog else []

            while True:
                cache_key = self._generate_cache_key(
                    "issues",
                    jql=jql_query,
                    fields=fields,
                    start_at=start_at,
                    max_results=max_results,
                    expand=",".join(expand),
                )
                cached_data = self._load_from_cache(cache_key)
                if cached_data:
                    self._logger.info(
                        f"Loaded issues from cache for JQL: {jql_query} (start_at={start_at})"
                    )
                    if isinstance(cached_data, dict):
                        current_issues = cached_data.get("issues", [])
                        issues.extend(current_issues)
                        
                        # Stop if we've fetched all available issues from cache
                        total_available = cached_data.get("total", 0)
                        if len(issues) >= total_available or len(current_issues) == 0:
                            break
                            
                        # Also stop if we received fewer issues than requested (indicates last page)
                        if len(current_issues) < max_results:
                            break
                            
                    elif isinstance(cached_data, list):
                        issues.extend(cached_data)
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
                        "expand": ",".join(expand),
                    },
                )

                if not response:
                    raise JiraQueryError("No response received from Jira API.", jql=jql_query)

                self._save_to_cache(cache_key, response)
                if isinstance(response, dict):
                    current_issues = response.get("issues", [])
                    issues.extend(current_issues)
                    
                    # Stop if we've fetched all available issues
                    total_available = response.get("total", 0)
                    if len(issues) >= total_available or len(current_issues) == 0:
                        break
                        
                    # Also stop if we received fewer issues than requested (indicates last page)
                    if len(current_issues) < max_results:
                        break
                        
                elif isinstance(response, list):
                    issues.extend(response)
                    break
                start_at += max_results

            return issues
        except JiraQueryError as e:
            self._logger.error(e)
            raise
        except Exception as e:
            raise JiraQueryError("Error fetching issues.", jql=jql_query, error=str(e)) from e

    def fetch_project_metadata(self, project_key: str) -> Dict:
        """
        Fetch project metadata including available issue types, statuses, etc.

        Args:
            project_key (str): The key of the Jira project.

        Returns:
            Dict: Project metadata including issue types, statuses, etc.
        """
        try:
            cache_key = self._generate_cache_key("project_metadata", project_key=project_key)
            cached_data = self._load_from_cache(cache_key)
            if cached_data:
                self._logger.info(f"Loaded project metadata from cache for project: {project_key}")
                return cached_data

            self._logger.info(f"Fetching project metadata for project: {project_key}")
            response = self.client.get(f"project/{project_key}")

            if not response:
                raise JiraMetadataFetchError(f"No metadata found for project {project_key}")

            self._save_to_cache(cache_key, response)
            return response
        except JiraMetadataFetchError as e:
            self._logger.error(e)
            raise
        except Exception as e:
            raise JiraMetadataFetchError(
                f"Error fetching project metadata for {project_key}", 
                project_key=project_key, 
                error=str(e)
            ) from e

    def fetch_project_issue_types(self, project_key: str) -> List[Dict]:
        """
        Fetch available issue types for a specific project.

        Args:
            project_key (str): The key of the Jira project.

        Returns:
            List[Dict]: List of issue types available in the project.
        """
        try:
            project_metadata = self.fetch_project_metadata(project_key)
            issue_types = project_metadata.get("issueTypes", [])
            
            self._logger.info(f"Found {len(issue_types)} issue types for project {project_key}")
            return issue_types
        except Exception as e:
            raise JiraMetadataFetchError(
                f"Error fetching issue types for project {project_key}", 
                project_key=project_key, 
                error=str(e)
            ) from e
