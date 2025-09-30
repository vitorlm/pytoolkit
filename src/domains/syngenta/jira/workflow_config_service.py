"""
JIRA Workflow Configuration Service

This service manages workflow configurations for different JIRA projects,
allowing customizable status mappings, transitions, and flow metrics.

Follows PyToolkit patterns:
- Uses LogManager singleton for logging
- Uses CacheManager for performance
- Uses JSONManager for configuration files
- Provides semantic status mapping abstraction
"""

import os
from typing import Optional

from utils.cache_manager.cache_manager import CacheManager
from utils.data.json_manager import JSONManager
from utils.logging.logging_manager import LogManager


class WorkflowConfigService:
    """Service for loading and managing workflow configurations."""

    def __init__(self, cache_expiration: int = 240):
        """
        Initialize workflow configuration service.

        Args:
            cache_expiration (int): Cache expiration in minutes (default: 4 hours)
        """
        self.logger = LogManager.get_instance().get_logger("WorkflowConfigService")
        self.cache = CacheManager.get_instance()
        self.cache_expiration = cache_expiration

        # Set up paths
        self._config_dir = os.path.join(os.path.dirname(__file__), "workflow_configs")
        self._mappings_file = os.path.join(self._config_dir, "project_mappings.json")

        # Load project mappings
        self._project_mappings = self._load_project_mappings()

        self.logger.info(
            f"WorkflowConfigService initialized with {len(self._project_mappings.get('project_mappings', {}))} project mappings"
        )

    def _load_project_mappings(self) -> dict:
        """Load project to workflow mappings."""
        try:
            if not os.path.exists(self._mappings_file):
                self.logger.warning(
                    f"Project mappings file not found: {self._mappings_file}"
                )
                return {
                    "project_mappings": {},
                    "default_workflow": "default_workflow.json",
                }

            return JSONManager.read_json(
                self._mappings_file,
                default={
                    "project_mappings": {},
                    "default_workflow": "default_workflow.json",
                },
            )
        except Exception as e:
            self.logger.error(f"Failed to load project mappings: {e}")
            return {"project_mappings": {}, "default_workflow": "default_workflow.json"}

    def get_workflow_config(self, project_key: str) -> dict:
        """
        Get workflow configuration for a project.

        Args:
            project_key (str): JIRA project key

        Returns:
            Dict: Workflow configuration
        """
        # Check cache first
        cache_key = f"workflow_config_{project_key}"
        cached_config = self.cache.load(
            cache_key, expiration_minutes=self.cache_expiration
        )

        if cached_config is not None:
            self.logger.info(f"Using cached workflow config for project {project_key}")
            return cached_config

        # Load from file
        config = self._load_workflow_config(project_key)

        # Cache the result
        self.cache.save(cache_key, config)
        self.logger.info(f"Cached workflow config for project {project_key}")

        return config

    def _load_workflow_config(self, project_key: str) -> dict:
        """Load workflow configuration from JSON file."""
        # Determine workflow file
        workflow_file = self._project_mappings.get("project_mappings", {}).get(
            project_key,
            self._project_mappings.get("default_workflow", "default_workflow.json"),
        )

        workflow_path = os.path.join(self._config_dir, workflow_file)

        if not os.path.exists(workflow_path):
            self.logger.warning(
                f"Workflow config file not found: {workflow_path}, using default"
            )
            workflow_path = os.path.join(self._config_dir, "default_workflow.json")

        if not os.path.exists(workflow_path):
            raise FileNotFoundError(
                f"No workflow configuration found for project {project_key}"
            )

        self.logger.info(f"Loading workflow config from {workflow_path}")

        # Load configuration
        config = JSONManager.read_json(workflow_path)

        # Apply custom field overrides if they exist
        custom_fields = self._project_mappings.get("custom_field_overrides", {}).get(
            project_key, {}
        )
        if custom_fields:
            config["custom_fields"].update(custom_fields)
            self.logger.info(
                f"Applied custom field overrides for {project_key}: {custom_fields}"
            )

        return config

    def is_wip_status(self, project_key: str, status_name: str) -> bool:
        """
        Check if a status is considered Work In Progress.

        Args:
            project_key (str): JIRA project key
            status_name (str): Status name from JIRA (e.g., "07 Started")

        Returns:
            bool: True if status is WIP
        """
        config = self.get_workflow_config(project_key)
        wip_statuses = config.get("status_mapping", {}).get("wip", [])
        return status_name in wip_statuses

    def is_done_status(self, project_key: str, status_name: str) -> bool:
        """
        Check if a status is considered Done.

        Args:
            project_key (str): JIRA project key
            status_name (str): Status name from JIRA

        Returns:
            bool: True if status is Done
        """
        config = self.get_workflow_config(project_key)
        done_statuses = config.get("status_mapping", {}).get("done", [])
        return status_name in done_statuses

    def is_backlog_status(self, project_key: str, status_name: str) -> bool:
        """
        Check if a status is considered Backlog.

        Args:
            project_key (str): JIRA project key
            status_name (str): Status name from JIRA

        Returns:
            bool: True if status is Backlog
        """
        config = self.get_workflow_config(project_key)
        backlog_statuses = config.get("status_mapping", {}).get("backlog", [])
        return status_name in backlog_statuses

    def get_semantic_status(
        self, project_key: str, semantic_name: str
    ) -> Optional[str]:
        """
        Get status name for semantic identifier.

        Args:
            project_key (str): JIRA project key
            semantic_name (str): Semantic name (e.g., "development_start", "completed")

        Returns:
            Optional[str]: Status name or None if not found
        """
        config = self.get_workflow_config(project_key)
        return config.get("semantic_statuses", {}).get(semantic_name)

    def get_wip_statuses(self, project_key: str) -> list[str]:
        """
        Get list of all WIP status names.

        Args:
            project_key (str): JIRA project key

        Returns:
            List[str]: List of WIP status names
        """
        config = self.get_workflow_config(project_key)
        return config.get("status_mapping", {}).get("wip", [])

    def get_done_statuses(self, project_key: str) -> list[str]:
        """
        Get list of all Done status names.

        Args:
            project_key (str): JIRA project key

        Returns:
            List[str]: List of Done status names
        """
        config = self.get_workflow_config(project_key)
        return config.get("status_mapping", {}).get("done", [])

    def get_backlog_statuses(self, project_key: str) -> list[str]:
        """
        Get list of all Backlog status names.

        Args:
            project_key (str): JIRA project key

        Returns:
            List[str]: List of Backlog status names
        """
        config = self.get_workflow_config(project_key)
        return config.get("status_mapping", {}).get("backlog", [])

    def get_active_statuses(self, project_key: str) -> list[str]:
        """
        Get list of all active (work-in-progress) status names.
        These are statuses where work is actively being done on an issue.

        Args:
            project_key (str): JIRA project key

        Returns:
            List[str]: List of active status names
        """
        config = self.get_workflow_config(project_key)
        return config.get("status_mapping", {}).get("wip", [])

    def get_waiting_statuses(self, project_key: str) -> list[str]:
        """
        Get list of all waiting status names.
        These are statuses where an issue is waiting for work to begin or resume.

        Args:
            project_key (str): JIRA project key

        Returns:
            List[str]: List of waiting status names
        """
        config = self.get_workflow_config(project_key)
        status_mapping = config.get("status_mapping", {})
        backlog_statuses = status_mapping.get("backlog", [])
        waiting_statuses = status_mapping.get(
            "waiting", []
        )  # For statuses like 'Blocked'
        return backlog_statuses + waiting_statuses

    def get_flow_metric_config(
        self, project_key: str, metric_name: str
    ) -> Optional[dict]:
        """
        Get flow metric configuration.

        Args:
            project_key (str): JIRA project key
            metric_name (str): Metric name (e.g., "cycle_time", "lead_time")

        Returns:
            Optional[Dict]: Metric configuration or None if not found
        """
        config = self.get_workflow_config(project_key)
        return config.get("flow_metrics", {}).get(metric_name)

    def get_cycle_time_statuses(self, project_key: str) -> tuple[str, str]:
        """
        Get cycle time start and end statuses.

        Args:
            project_key (str): JIRA project key

        Returns:
            tuple[str, str]: (start_status, end_status)
        """
        cycle_config = self.get_flow_metric_config(project_key, "cycle_time")
        if not cycle_config:
            raise ValueError(
                f"No cycle_time configuration found for project {project_key}"
            )

        start_status = cycle_config.get("start")
        end_status = cycle_config.get("end")

        if not start_status or not end_status:
            raise ValueError(
                f"Invalid cycle_time configuration for project {project_key}"
            )

        return start_status, end_status

    def get_lead_time_statuses(self, project_key: str) -> tuple[str, str]:
        """
        Get lead time start and end statuses.

        Args:
            project_key (str): JIRA project key

        Returns:
            tuple[str, str]: (start_status, end_status)
        """
        lead_config = self.get_flow_metric_config(project_key, "lead_time")
        if not lead_config:
            raise ValueError(
                f"No lead_time configuration found for project {project_key}"
            )

        start_status = lead_config.get("start")
        end_status = lead_config.get("end")

        if not start_status or not end_status:
            raise ValueError(
                f"Invalid lead_time configuration for project {project_key}"
            )

        return start_status, end_status

    def get_custom_field(self, project_key: str, field_name: str) -> Optional[str]:
        """
        Get custom field ID for a project.

        Args:
            project_key (str): JIRA project key
            field_name (str): Field name (e.g., "squad_field", "epic_link_field")

        Returns:
            Optional[str]: Custom field ID or None if not found
        """
        config = self.get_workflow_config(project_key)
        return config.get("custom_fields", {}).get(field_name)

    def get_quality_gates(self, project_key: str) -> dict:
        """
        Get quality gates configuration.

        Args:
            project_key (str): JIRA project key

        Returns:
            Dict: Quality gates configuration
        """
        config = self.get_workflow_config(project_key)
        return config.get("quality_gates", {})

    def validate_workflow_config(self, project_key: str) -> dict[str, list[str]]:
        """
        Validate workflow configuration for consistency.

        Args:
            project_key (str): JIRA project key

        Returns:
            Dict[str, List[str]]: Validation results with errors and warnings
        """
        validation_results: dict[str, list[str]] = {"errors": [], "warnings": []}

        try:
            config = self.get_workflow_config(project_key)

            # Check required sections
            required_sections = ["status_mapping", "semantic_statuses", "flow_metrics"]
            for section in required_sections:
                if section not in config:
                    validation_results["errors"].append(
                        f"Missing required section: {section}"
                    )

            # Check flow metrics reference valid statuses
            all_statuses = set()
            for category_statuses in config.get("status_mapping", {}).values():
                all_statuses.update(category_statuses)

            flow_metrics = config.get("flow_metrics", {})
            for metric_name, metric_config in flow_metrics.items():
                start_status = metric_config.get("start")
                end_status = metric_config.get("end")

                if start_status and start_status not in all_statuses:
                    validation_results["errors"].append(
                        f"Flow metric '{metric_name}' start status '{start_status}' not found in status_mapping"
                    )

                if end_status:
                    if isinstance(end_status, list):
                        for status in end_status:
                            if status not in all_statuses:
                                validation_results["errors"].append(
                                    f"Flow metric '{metric_name}' end status '{status}' not found in status_mapping"
                                )
                    elif end_status not in all_statuses:
                        validation_results["errors"].append(
                            f"Flow metric '{metric_name}' end status '{end_status}' not found in status_mapping"
                        )

            # Check semantic statuses reference valid statuses
            for semantic_name, status_name in config.get(
                "semantic_statuses", {}
            ).items():
                if status_name not in all_statuses:
                    validation_results["warnings"].append(
                        f"Semantic status '{semantic_name}' references unknown status '{status_name}'"
                    )

            self.logger.info(
                f"Validation completed for {project_key}: {len(validation_results['errors'])} errors, {len(validation_results['warnings'])} warnings"
            )

        except Exception as e:
            validation_results["errors"].append(f"Failed to validate config: {str(e)}")

        return validation_results

    def get_available_projects(self) -> list[str]:
        """
        Get list of available project configurations.

        Returns:
            List[str]: List of project keys with configurations
        """
        return list(self._project_mappings.get("project_mappings", {}).keys())

    def clear_cache(self, project_key: Optional[str] = None):
        """
        Clear workflow configuration cache.

        Args:
            project_key (Optional[str]): Project key to clear or None for all
        """
        if project_key:
            cache_key = f"workflow_config_{project_key}"
            self.cache.invalidate(cache_key)
            self.logger.info(f"Cleared workflow config cache for {project_key}")
        else:
            # Clear all cache entries since CacheManager doesn't have pattern clearing
            self.cache.clear_all()
            self.logger.info("Cleared all workflow config cache")

    def get_status_category(self, project_key: str, status_name: str) -> Optional[str]:
        """
        Get the category (backlog, wip, done, archived) for a status.

        Args:
            project_key (str): JIRA project key
            status_name (str): Status name from JIRA

        Returns:
            Optional[str]: Status category or None if not found
        """
        config = self.get_workflow_config(project_key)
        status_mapping = config.get("status_mapping", {})

        for category, statuses in status_mapping.items():
            if status_name in statuses:
                return category

        self.logger.warning(
            f"Status '{status_name}' not found in any category for project {project_key}"
        )
        return None
