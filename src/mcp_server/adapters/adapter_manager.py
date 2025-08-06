"""
Adapter Manager for MCP Services.

This module provides a centralized manager for all MCP adapters,
enabling easy access and management of different service adapters.
"""

from typing import Any, Optional

from utils.logging.logging_manager import LogManager

from .base_adapter import BaseAdapter
from .circleci_adapter import CircleCIAdapter
from .jira_adapter import JiraAdapter
from .linearb_adapter import LinearBAdapter
from .sonarqube_adapter import SonarQubeAdapter


class AdapterManager:
    """
    Centralized manager for all MCP service adapters.

    Provides singleton access to adapters with lazy initialization
    and centralized health monitoring.
    """

    _instance: Optional["AdapterManager"] = None

    # Registry of available adapters
    _adapter_registry: dict[str, type[BaseAdapter]] = {
        "jira": JiraAdapter,
        "sonarqube": SonarQubeAdapter,
        "circleci": CircleCIAdapter,
        "linearb": LinearBAdapter,
    }

    def __init__(self) -> None:
        if AdapterManager._instance is not None:
            raise RuntimeError("AdapterManager is a singleton. Use get_instance() instead.")

        self.logger = LogManager.get_instance().get_logger("AdapterManager")
        self._adapters: dict[str, BaseAdapter] = {}

        self.logger.info("AdapterManager initialized")

    @classmethod
    def get_instance(cls) -> "AdapterManager":
        """Get singleton instance of AdapterManager."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def get_adapter(self, adapter_name: str) -> BaseAdapter:
        """
        Get adapter instance by name with lazy initialization.

        Args:
            adapter_name: Name of the adapter (jira, sonarqube, circleci)

        Returns:
            Adapter instance

        Raises:
            ValueError: If adapter name is not registered
        """
        if adapter_name not in self._adapter_registry:
            available = ", ".join(self._adapter_registry.keys())
            raise ValueError(f"Unknown adapter '{adapter_name}'. Available: {available}")

        # Lazy initialization
        if adapter_name not in self._adapters:
            adapter_class = self._adapter_registry[adapter_name]
            self.logger.info(f"Initializing {adapter_name} adapter")

            try:
                # Type ignore because mypy doesn't understand that concrete adapter classes
                # don't require the adapter_name parameter (only BaseAdapter does)
                self._adapters[adapter_name] = adapter_class()  # type: ignore
                self.logger.info(f"{adapter_name} adapter initialized successfully")
            except Exception as e:
                self.logger.error(f"Failed to initialize {adapter_name} adapter: {e}")
                raise

        return self._adapters[adapter_name]

    def get_jira_adapter(self) -> JiraAdapter:
        """Get JIRA adapter with type safety."""
        adapter = self.get_adapter("jira")
        assert isinstance(adapter, JiraAdapter)
        return adapter

    def get_sonarqube_adapter(self) -> SonarQubeAdapter:
        """Get SonarQube adapter with type safety."""
        adapter = self.get_adapter("sonarqube")
        assert isinstance(adapter, SonarQubeAdapter)
        return adapter

    def get_circleci_adapter(self) -> CircleCIAdapter:
        """Get CircleCI adapter with type safety."""
        adapter = self.get_adapter("circleci")
        assert isinstance(adapter, CircleCIAdapter)
        return adapter

    def get_linearb_adapter(self) -> LinearBAdapter:
        """Get LinearB adapter with type safety."""
        adapter = self.get_adapter("linearb")
        assert isinstance(adapter, LinearBAdapter)
        return adapter

    def get_available_adapters(self) -> dict[str, str]:
        """
        Get list of available adapters with their descriptions.

        Returns:
            Dictionary mapping adapter names to descriptions
        """
        descriptions = {}

        for name, adapter_class in self._adapter_registry.items():
            # Get class name as description since adapters don't take constructor args
            descriptions[name] = f"{adapter_class.__name__} - {name.title()} Integration"

        return descriptions

    def health_check_all(self) -> dict[str, Any]:
        """
        Perform health check on all initialized adapters.

        Returns:
            Health status for all adapters
        """
        health_status = {
            "adapter_manager": {
                "status": "healthy",
                "total_adapters_available": len(self._adapter_registry),
                "initialized_adapters": len(self._adapters),
                "available_adapters": list(self._adapter_registry.keys()),
            },
            "adapters": {},
        }

        # Check health of initialized adapters
        for name, adapter in self._adapters.items():
            try:
                self.logger.info(f"Checking health of {name} adapter")
                adapter_health = adapter.health_check()
                health_status["adapters"][name] = adapter_health
            except Exception as e:
                self.logger.error(f"Health check failed for {name} adapter: {e}")
                health_status["adapters"][name] = {
                    "adapter": name,
                    "status": "unhealthy",
                    "error": str(e),
                }

        # Summary
        healthy_adapters = sum(
            1
            for adapter_health in health_status["adapters"].values()
            if isinstance(adapter_health, dict) and adapter_health.get("status") == "healthy"
        )

        health_status["adapter_manager"].update(
            {
                "healthy_adapters": healthy_adapters,
                "unhealthy_adapters": len(self._adapters) - healthy_adapters,
            }
        )

        return health_status

    def clear_all_caches(self) -> dict[str, Any]:
        """
        Clear caches for all initialized adapters.

        Returns:
            Status of cache clearing operations
        """
        clear_results = {
            "adapter_manager": {
                "operation": "clear_all_caches",
                "initialized_adapters": len(self._adapters),
            },
            "results": {},
        }

        for name, adapter in self._adapters.items():
            try:
                self.logger.info(f"Clearing cache for {name} adapter")
                result = adapter.clear_cache()
                clear_results["results"][name] = result
            except Exception as e:
                self.logger.error(f"Failed to clear cache for {name} adapter: {e}")
                clear_results["results"][name] = {
                    "adapter": name,
                    "status": "error",
                    "error": str(e),
                }

        return clear_results

    def get_comprehensive_status(self) -> dict[str, Any]:
        """
        Get comprehensive status of all adapters and their capabilities.

        Returns:
            Comprehensive status information
        """
        return {
            "adapter_manager": {
                "total_available": len(self._adapter_registry),
                "initialized": len(self._adapters),
                "registry": list(self._adapter_registry.keys()),
            },
            "health_check": self.health_check_all(),
            "capabilities": {
                "jira": [
                    "epic_monitoring",
                    "cycle_time_analysis",
                    "velocity_analysis",
                    "adherence_analysis",
                    "resolution_time_analysis",
                    "comprehensive_dashboard",
                ],
                "sonarqube": [
                    "projects_with_metrics",
                    "project_details",
                    "quality_dashboard",
                    "available_metrics",
                ],
                "circleci": [
                    "pipeline_status",
                    "build_metrics",
                    "deployment_frequency_analysis",
                    "list_projects",
                    "health_check",
                ],
                "linearb": [
                    "engineering_metrics",
                    "team_performance",
                    "teams_info",
                    "available_metrics",
                    "health_status",
                ],
            },
        }
