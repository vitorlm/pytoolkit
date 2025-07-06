"""
SonarQube Service for fetching project metrics and issues.
"""

import requests
from typing import Dict, List, Optional
from utils.logging.logging_manager import LogManager
from utils.data.json_manager import JSONManager
from utils.cache_manager.cache_manager import CacheManager
import os
import hashlib


class SonarQubeService:
    """
    Service class for interacting with SonarQube API.
    """

    def __init__(self, base_url: Optional[str] = None, token: Optional[str] = None):
        """
        Initialize the SonarQube service.

        Args:
            base_url: SonarQube instance URL (defaults to env SONARQUBE_URL)
            token: Authentication token (defaults to env SONARQUBE_TOKEN)
        """
        # Load SonarQube-specific environment file if it exists
        sonarqube_env_path = os.path.join(os.path.dirname(__file__), ".env")
        if os.path.exists(sonarqube_env_path):
            from dotenv import load_dotenv

            load_dotenv(sonarqube_env_path)

        self.base_url = base_url or os.getenv("SONARQUBE_URL", "https://sonarcloud.io")
        self.token = token or os.getenv("SONARQUBE_TOKEN")
        self.organization = os.getenv("SONARQUBE_ORGANIZATION", "syngenta-digital")
        self.session = requests.Session()

        if self.token:
            self.session.auth = (self.token, "")

        self._logger = LogManager.get_instance().get_logger("SonarQubeService")
        self._cache_manager = CacheManager.get_instance()
        self._cache_expiration = 60  # 1 hour cache expiration in minutes

    def get_projects(
        self,
        organization: Optional[str] = None,
        use_project_list: bool = False,
        output_file: Optional[str] = None,
    ) -> List[Dict]:
        """
        Fetch all projects from SonarQube with pagination support.

        Args:
            organization: Organization key (for SonarCloud)
            use_project_list: Whether to use predefined project list instead of fetching all
            output_file: Optional file to save results

        Returns:
            List of project dictionaries
        """
        # If using project list, load projects from file
        if use_project_list:
            projects_file = os.path.join(os.path.dirname(__file__), "projects_list.json")
            if os.path.exists(projects_file):
                self._logger.info("Using predefined project list")
                projects_data = JSONManager.read_json(projects_file)
                project_keys = projects_data.get("projects", [])
                return self._get_projects_by_keys(project_keys, organization, output_file)
            else:
                self._logger.warning(f"Projects list file not found: {projects_file}")
                return []

        # Generate cache key for pagination request
        cache_key = self._generate_cache_key(
            "projects_search",
            organization=organization or self.organization,
            use_project_list=use_project_list,
        )

        # Try to get from cache first
        try:
            cached_result = self._cache_manager.load(
                cache_key, expiration_minutes=self._cache_expiration
            )
            if cached_result is not None and len(cached_result) > 0:
                self._logger.info(
                    f"Using cached projects data (found {len(cached_result)} projects)"
                )
                if output_file:
                    JSONManager.write_json(cached_result, output_file)
                    self._logger.info(f"Projects saved to '{output_file}'")
                return cached_result
            elif cached_result is not None:
                self._logger.info("Found empty cached result, proceeding with API call")
        except Exception as e:
            self._logger.warning(f"Failed to load cached data: {e}")

        url = f"{self.base_url}/api/projects/search"
        all_projects = []
        page = 1
        page_size = 500  # Maximum page size

        # Use provided organization or fall back to default
        org = organization or self.organization

        try:
            while True:
                params: Dict[str, str | int] = {"p": page, "ps": page_size}
                if org:
                    params["organization"] = org

                # Log the request details
                self._logger.debug(f"Requesting page {page} - URL: {url}")
                self._logger.debug(f"Parameters: {params}")

                response = self.session.get(url, params=params)
                response.raise_for_status()

                data = response.json()
                projects = data.get("components", [])
                paging = data.get("paging", {})
                total = paging.get("total", 0)
                page_index = paging.get("pageIndex", page)
                page_size_returned = paging.get("pageSize", page_size)

                self._logger.info(
                    f"Page {page_index}, Total: {total}, "
                    f"Page size: {page_size_returned}, Returned: {len(projects)}"
                )

                # Add projects from this page
                all_projects.extend(projects)

                # Check if we have more pages
                if len(projects) < page_size or len(all_projects) >= total:
                    break

                page += 1

            self._logger.info(f"Fetched {len(all_projects)} total projects from {page} pages")

            self._logger.info(f"Found {len(all_projects)} projects")
            for project in all_projects:
                self._logger.debug(f"  - {project.get('key')}: {project.get('name')}")

            # Cache the results
            try:
                self._cache_manager.save(cache_key, all_projects)
                self._logger.debug(f"Cached projects data with key: {cache_key}")
            except Exception as e:
                self._logger.warning(f"Failed to cache projects data: {e}")

            if output_file:
                JSONManager.write_json(all_projects, output_file)
                self._logger.info(f"Projects saved to '{output_file}'")

            self._logger.info(f"Fetched {len(all_projects)} projects")
            return all_projects

        except requests.exceptions.RequestException as e:
            self._logger.error(f"Failed to fetch projects: {e}")
            return []

    def get_project_measures(
        self, project_key: str, metric_keys: List[str], output_file: Optional[str] = None
    ) -> Dict:
        """
        Fetch measures for a specific project.

        Args:
            project_key: Project key
            metric_keys: List of metric keys to fetch
            output_file: Optional file to save results

        Returns:
            Dictionary containing project measures
        """
        url = f"{self.base_url}/api/measures/component"
        params = {"component": project_key, "metricKeys": ",".join(metric_keys)}

        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            component = data.get("component", {})
            measures = component.get("measures", [])

            self._logger.info(f"Measures for project '{project_key}':")
            for measure in measures:
                metric = measure.get("metric")
                value = measure.get("value")
                self._logger.info(f"{metric}: {value}")

            if output_file:
                JSONManager.write_json(data, output_file)
                self._logger.info(f"Measures saved to '{output_file}'")

            self._logger.info(f"Fetched {len(measures)} measures for project '{project_key}'")
            return data

        except requests.exceptions.RequestException as e:
            self._logger.error(f"Failed to fetch measures for project '{project_key}': {e}")
            return {}

    def get_project_issues(
        self,
        project_key: str,
        severities: Optional[List[str]] = None,
        output_file: Optional[str] = None,
    ) -> List[Dict]:
        """
        Fetch issues for a specific project.

        Args:
            project_key: Project key
            severities: List of severities to filter by (INFO, MINOR, MAJOR, CRITICAL, BLOCKER)
            output_file: Optional file to save results

        Returns:
            List of issue dictionaries
        """
        url = f"{self.base_url}/api/issues/search"
        params = {"componentKeys": project_key, "ps": 500}  # Page size

        if severities:
            params["severities"] = ",".join(severities)

        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            issues = data.get("issues", [])

            self._logger.info(f"Found {len(issues)} issues in project '{project_key}':")

            severity_counts = {}
            for issue in issues:
                severity = issue.get("severity")
                severity_counts[severity] = severity_counts.get(severity, 0) + 1

            for severity, count in severity_counts.items():
                self._logger.info(f"{severity}: {count}")

            if output_file:
                JSONManager.write_json(issues, output_file)
                self._logger.info(f"Issues saved to '{output_file}'")

            self._logger.info(f"Fetched {len(issues)} issues for project '{project_key}'")
            return issues

        except requests.exceptions.RequestException as e:
            self._logger.error(f"Failed to fetch issues for project '{project_key}': {e}")
            return []

    def get_available_metrics(self, output_file: Optional[str] = None) -> List[Dict]:
        """
        Fetch all available metrics.

        Args:
            output_file: Optional file to save results

        Returns:
            List of available metrics
        """
        url = f"{self.base_url}/api/metrics/search"
        params = {"ps": 500}

        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            metrics = data.get("metrics", [])

            self._logger.info(f"Available metrics ({len(metrics)}):")
            for metric in metrics:
                key = metric.get("key")
                name = metric.get("name")
                type_val = metric.get("type")
                self._logger.debug(f"{key}: {name} ({type_val})")

            if output_file:
                JSONManager.write_json(metrics, output_file)
                self._logger.info(f"Metrics saved to '{output_file}'")

            self._logger.info(f"Fetched {len(metrics)} available metrics")
            return metrics

        except requests.exceptions.RequestException as e:
            self._logger.error(f"Failed to fetch available metrics: {e}")
            return []

    def get_batch_measures(
        self, project_keys: List[str], metric_keys: List[str], output_file: Optional[str] = None
    ) -> Dict:
        """
        Fetch measures for multiple projects in a single request.

        Args:
            project_keys: List of project keys
            metric_keys: List of metric keys to fetch
            output_file: Optional file to save results

        Returns:
            Dictionary containing batch measures data
        """
        url = f"{self.base_url}/api/measures/search"
        params = {"projectKeys": ",".join(project_keys), "metricKeys": ",".join(metric_keys)}

        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            measures = data.get("measures", [])

            self._logger.info(f"Batch measures for {len(project_keys)} projects:")

            # Group measures by project
            projects_data = {}
            for measure in measures:
                project_key = measure.get("component")
                if project_key not in projects_data:
                    projects_data[project_key] = {}

                metric = measure.get("metric")
                value = measure.get("value")
                projects_data[project_key][metric] = value

            # Log results summary
            for project_key, metrics_data in projects_data.items():
                self._logger.info(f"Project: {project_key}")
                for metric, value in metrics_data.items():
                    self._logger.debug(f"  {metric}: {value}")

            if output_file:
                JSONManager.write_json(data, output_file)
                self._logger.info(f"Batch measures saved to '{output_file}'")

            self._logger.info(f"Fetched batch measures for {len(project_keys)} projects")
            return data

        except requests.exceptions.RequestException as e:
            self._logger.error(f"Failed to fetch batch measures: {e}")
            return {}

    def get_projects_by_list(
        self,
        organization: Optional[str] = None,
        include_measures: bool = False,
        metric_keys: Optional[List[str]] = None,
        output_file: Optional[str] = None,
    ) -> Dict:
        """
        Get projects from predefined list and optionally include their measures.

        Args:
            organization: Organization key (for SonarCloud)
            include_measures: Whether to fetch measures for the projects
            metric_keys: List of metric keys to fetch (if include_measures is True)
            output_file: Optional file to save results

        Returns:
            Dictionary with projects and optionally their measures
        """
        # First get projects from predefined list
        projects = self.get_projects(organization=organization, use_project_list=True)

        result = {
            "filters": {"organization": organization, "use_project_list": True},
            "projects": [],
        }

        if include_measures and projects:
            # Get project keys and filter out None values
            project_keys = [project.get("key") for project in projects]
            project_keys = [key for key in project_keys if isinstance(key, str) and key is not None]

            if project_keys and metric_keys:
                self._logger.info(
                    f"Fetching measures for {len(project_keys)} projects from list..."
                )
                measures_data = self.get_batch_measures(project_keys, metric_keys)
                # Create a mapping of project key to measures
                project_measures = {}
                for measure in measures_data.get("measures", []):
                    project_key = measure.get("component")
                    if project_key not in project_measures:
                        project_measures[project_key] = {}
                    metric = measure.get("metric")
                    value = measure.get("value")
                    best_value = measure.get("bestValue", False)
                    project_measures[project_key][metric] = {
                        "value": value,
                        "bestValue": best_value,
                    }

                # Combine projects with their measures
                for project in projects:
                    project_key = project.get("key")
                    project_info = {
                        "key": project_key,
                        "name": project.get("name"),
                        "qualifier": project.get("qualifier"),
                        "visibility": project.get("visibility"),
                        "lastAnalysisDate": project.get("lastAnalysisDate"),
                        "measures": project_measures.get(project_key, {}),
                    }
                    result["projects"].append(project_info)
            elif include_measures and not metric_keys:
                self._logger.warning("include_measures=True but no metric_keys provided")
                # Add projects without measures
                result["projects"] = projects
        else:
            # Add projects without measures
            result["projects"] = projects

        if output_file:
            JSONManager.write_json(result, output_file)
            self._logger.info(f"Projects list data saved to '{output_file}'")

        return result

    def _get_projects_by_keys(
        self,
        project_keys: List[str],
        organization: Optional[str] = None,
        output_file: Optional[str] = None,
    ) -> List[Dict]:
        """
        Get specific projects by their keys using batch requests.

        Args:
            project_keys: List of project keys to fetch
            organization: Organization key (for SonarCloud)
            output_file: Optional file to save results

        Returns:
            List of project dictionaries
        """
        if not project_keys:
            return []

        # Generate cache key for project keys request
        cache_key = self._generate_cache_key(
            "projects_by_keys",
            project_keys=sorted(project_keys),  # Sort for consistent cache key
            organization=organization or self.organization,
        )

        # Try to get from cache first
        try:
            cached_result = self._cache_manager.load(
                cache_key, expiration_minutes=self._cache_expiration
            )
            if cached_result is not None and len(cached_result) > 0:
                self._logger.info(
                    f"Using cached project keys data (found {len(cached_result)} projects)"
                )
                if output_file:
                    JSONManager.write_json(cached_result, output_file)
                    self._logger.info(f"Projects saved to '{output_file}'")
                return cached_result
            elif cached_result is not None:
                self._logger.info(
                    "Found empty cached project keys result, proceeding with API call"
                )
        except Exception as e:
            self._logger.warning(f"Failed to load cached project keys data: {e}")

        # Split into chunks of 500 (API limit for projects parameter)
        chunk_size = 500
        all_projects = []

        self._logger.info(f"Fetching data for {len(project_keys)} project keys")

        for i in range(0, len(project_keys), chunk_size):
            chunk = project_keys[i : i + chunk_size]
            url = f"{self.base_url}/api/projects/search"

            params = {"projects": ",".join(chunk), "ps": 500}

            org = organization or self.organization
            if org:
                params["organization"] = org

            try:
                chunk_num = i // chunk_size + 1
                self._logger.info(f"Fetching chunk {chunk_num}: {len(chunk)} projects")
                response = self.session.get(url, params=params)
                response.raise_for_status()

                data = response.json()
                projects = data.get("components", [])
                all_projects.extend(projects)

                self._logger.debug(f"Found {len(projects)} projects in chunk")

            except requests.exceptions.RequestException as e:
                self._logger.error(f"Failed to fetch project chunk: {e}")

        self._logger.info(f"Found {len(all_projects)} total projects from predefined list")
        for project in all_projects:
            self._logger.debug(f"  - {project.get('key')}: {project.get('name')}")

        # Cache the results
        try:
            self._cache_manager.save(cache_key, all_projects)
            self._logger.debug(f"Cached project keys data with key: {cache_key}")
        except Exception as e:
            self._logger.warning(f"Failed to cache project keys data: {e}")

        if output_file:
            JSONManager.write_json(all_projects, output_file)
            self._logger.info(f"Projects saved to '{output_file}'")

        self._logger.info(f"Fetched {len(all_projects)} projects from predefined list")
        return all_projects

    def _generate_cache_key(self, prefix: str, **kwargs) -> str:
        """
        Generate a cache key based on a prefix and additional parameters.

        Args:
            prefix: The base prefix for the cache key
            **kwargs: Additional parameters to include in the cache key

        Returns:
            String cache key
        """
        # Sort the items and convert them to a JSON string for deterministic ordering
        sorted_items = JSONManager.create_json(kwargs)
        # Create a consistent hash using SHA-256
        hash_object = hashlib.sha256(sorted_items.encode("utf-8"))
        # Convert the hash to a hexadecimal string
        hash_hex = hash_object.hexdigest()
        return f"sonarqube_{prefix}_{hash_hex}"

    def clear_cache(self):
        """
        Clear all SonarQube related cache entries.
        """
        try:
            self._cache_manager.clear_all()
            self._logger.info("Cache cleared successfully")
        except Exception as e:
            self._logger.warning(f"Failed to clear cache: {e}")
