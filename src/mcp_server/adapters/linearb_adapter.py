"""LinearB Adapter for MCP Integration.

This module provides the actual LinearB adapter integration,
using the existing PyToolkit LinearB infrastructure.
"""

import os
import sys
from argparse import Namespace
from datetime import datetime
from typing import Any

# Add src to path to import PyToolkit domains
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from domains.linearb.linearb_service import LinearBService
from utils.env_loader import ensure_linearb_env_loaded

from .base_adapter import BaseAdapter


class LinearBAdapter(BaseAdapter):
    """Adapter for LinearB integration via PyToolkit.

    This adapter uses the existing LinearB infrastructure from PyToolkit
    to provide engineering metrics and team performance data.
    """

    def __init__(self) -> None:
        """Initialize LinearB adapter."""
        super().__init__("LinearB")
        # Load LinearB specific environment variables
        ensure_linearb_env_loaded()

        # Initialize service property
        self._linearb_service: LinearBService | None = None

        self.logger.info("LinearB Adapter initialized with PyToolkit integration")

    def initialize_service(self) -> dict[str, Any]:
        """Initialize LinearB service using PyToolkit infrastructure."""
        try:
            # Check for required environment variables first
            if not os.getenv("LINEARB_API_KEY"):  # Fixed env var name
                self.logger.warning("LINEARB_API_KEY not configured - LinearB features unavailable")
                return {
                    "service_name": "linearb_pytoolkit",
                    "status": "not_configured",
                    "error": "LINEARB_API_TOKEN environment variable is required",
                    "initialized_at": datetime.now().isoformat(),
                }

            self._linearb_service = LinearBService()

            # Test connection to validate service
            if not self._linearb_service:
                raise ValueError("Failed to initialize LinearB service")
            connection_test = self._linearb_service.test_connection()

            service_info = {
                "service_name": "linearb_pytoolkit",
                "status": "active",
                "connection_test": connection_test,
                "initialized_at": datetime.now().isoformat(),
            }

            self.logger.info("LinearB service initialized successfully with PyToolkit")
            return service_info

        except Exception as e:
            self.logger.error(f"Failed to initialize LinearB service: {e}")
            return {"error": str(e), "status": "failed"}

    def _load_environment(self) -> None:
        """Loads LinearB-specific environment variables.

        This method is responsible for loading and validating LinearB-specific
        configuration from environment variables using PyToolkit's env_loader.

        Returns:
            None
        """
        try:
            # Load LinearB specific environment variables
            ensure_linearb_env_loaded()

            # Check for required LinearB environment variables
            required_vars = ["LINEARB_API_KEY"]  # Fixed: using correct env var name
            missing_vars = []

            for var in required_vars:
                if not os.getenv(var):
                    missing_vars.append(var)

            if missing_vars:
                self.logger.debug(f"Missing LinearB environment variables: {missing_vars}")
            else:
                self.logger.debug("LinearB environment variables loaded successfully")

        except Exception as e:
            self.logger.error(f"Failed to load LinearB environment: {e}")

    def _validate_configuration(self) -> bool:
        """Validates LinearB configuration.

        This method validates that all required LinearB configuration
        parameters are properly set and accessible.

        Returns:
            bool: True if configuration is valid, False otherwise.
        """
        try:
            # Check required environment variables
            api_token = os.getenv("LINEARB_API_TOKEN")
            api_url = os.getenv("LINEARB_API_URL")

            if not api_token:
                self.logger.error("LINEARB_API_TOKEN not found in environment")
                return False

            if not api_url:
                self.logger.error("LINEARB_API_URL not found in environment")
                return False

            # Try to initialize service to validate configuration
            if not self._linearb_service:
                self._linearb_service = LinearBService()
                if not self._linearb_service:
                    raise ValueError("Failed to initialize LinearB service")

            self.logger.debug("LinearB configuration validated successfully")
            return True

        except Exception as e:
            self.logger.error(f"LinearB configuration validation failed: {e}")
            return False

    def get_health_status(self) -> dict[str, Any]:
        """Gets LinearB service health status.

        This method checks the health and availability of the LinearB service
        and returns status information using the PyToolkit LinearB service.

        Returns:
            dict[str, Any]: Health status information including service status,
                          timestamp, version, and any relevant health metrics.
        """
        try:
            if not self._linearb_service:
                self._linearb_service = LinearBService()
                if not self._linearb_service:
                    raise ValueError("Failed to initialize LinearB service")

            # Test connection
            connection_test = self._linearb_service.test_connection()

            status = "healthy" if connection_test.get("success", False) else "unhealthy"

            return {
                "service": "LinearB",
                "status": status,
                "timestamp": datetime.now().isoformat(),
                "version": "pytoolkit-integrated",
                "connection_test": connection_test,
                "configuration_valid": self._validate_configuration(),
            }

        except Exception as e:
            self.logger.error(f"LinearB health check failed: {e}")
            return {
                "service": "LinearB",
                "status": "error",
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
            }

    def get_engineering_metrics(
        self, time_range: str = "last-week", team_ids: list[str] | None = None
    ) -> dict[str, Any]:
        """Gets engineering metrics from LinearB.

        Args:
            time_range: Analysis period (last-week, last-month, etc.)
            team_ids: Team IDs for analysis (optional)

        Returns:
            dict with engineering metrics
        """

        def _get_engineering_metrics(**kwargs) -> dict[str, Any]:
            try:
                if not self._linearb_service:
                    self._linearb_service = LinearBService()
                    if not self._linearb_service:
                        raise ValueError("Failed to initialize LinearB service")

                # Create args namespace for the service
                args = Namespace(
                    time_range=kwargs.get("time_range", "last-week"),
                    team_ids=kwargs.get("team_ids"),
                    format="json",
                    output_folder="cache",
                    aggregation="p75",
                    granularity="custom",
                    filter_type="team",
                )

                # Get engineering metrics using the service
                self._linearb_service.get_engineering_metrics(args)

                # Also get performance metrics for comprehensive data
                performance_data = self._linearb_service.get_performance_metrics(args)

                return {
                    "time_range": kwargs.get("time_range"),
                    "team_ids": kwargs.get("team_ids"),
                    "metrics": performance_data.get("metrics", {}),
                    "parameters": performance_data.get("parameters", {}),
                    "timestamp": datetime.now().isoformat(),
                    "source": "linearb_pytoolkit",
                }

            except Exception as e:
                self.logger.error(f"Failed to get engineering metrics: {e}")
                return {
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                    "source": "linearb_pytoolkit",
                }

        return self.cached_operation(
            "engineering_metrics",
            _get_engineering_metrics,
            expiration_minutes=60,
            time_range=time_range,
            team_ids=team_ids,
        )

    def get_team_performance(self, team_ids: list[str] | None = None) -> dict[str, Any]:
        """Gets team performance analysis.

        Args:
            team_ids: Team IDs for analysis

        Returns:
            dict with team performance data
        """

        def _get_team_performance(**kwargs) -> dict[str, Any]:
            try:
                if not self._linearb_service:
                    self._linearb_service = LinearBService()
                    if not self._linearb_service:
                        raise ValueError("Failed to initialize LinearB service")

                # Create args namespace for the service
                args = Namespace(
                    time_range="last-month",
                    team_ids=kwargs.get("team_ids"),
                    format="json",
                    aggregation="p75",
                    granularity="custom",
                    filter_type="team",
                )

                # Get performance metrics
                performance_data = self._linearb_service.get_performance_metrics(args)

                # Generate summary for easier consumption
                summary = self._linearb_service.get_team_performance_summary(performance_data)

                return {
                    "team_ids": kwargs.get("team_ids"),
                    "performance_data": performance_data.get("metrics", {}),
                    "summary": summary,
                    "parameters": performance_data.get("parameters", {}),
                    "timestamp": datetime.now().isoformat(),
                    "source": "linearb_pytoolkit",
                }

            except Exception as e:
                self.logger.error(f"Failed to get team performance: {e}")
                return {
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                    "source": "linearb_pytoolkit",
                }

        return self.cached_operation(
            "team_performance",
            _get_team_performance,
            expiration_minutes=120,
            team_ids=team_ids,
        )

    def get_teams_info(self, search_term: str | None = None) -> dict[str, Any]:
        """Gets teams information from LinearB.

        Args:
            search_term: Optional search term to filter teams

        Returns:
            dict with teams information
        """

        def _get_teams_info(**kwargs) -> dict[str, Any]:
            try:
                if not self._linearb_service:
                    self._linearb_service = LinearBService()
                    if not self._linearb_service:
                        raise ValueError("Failed to initialize LinearB service")

                teams_data = self._linearb_service.get_teams_info(kwargs.get("search_term"))

                return {
                    "search_term": kwargs.get("search_term"),
                    "teams": teams_data,
                    "timestamp": datetime.now().isoformat(),
                    "source": "linearb_pytoolkit",
                }

            except Exception as e:
                self.logger.error(f"Failed to get teams info: {e}")
                return {
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                    "source": "linearb_pytoolkit",
                }

        return self.cached_operation(
            "teams_info",
            _get_teams_info,
            expiration_minutes=240,  # Teams info changes less frequently
            search_term=search_term,
        )

    def get_available_metrics(self) -> dict[str, Any]:
        """Gets available metrics from LinearB.

        Returns:
            dict with available metrics organized by category
        """
        try:
            if not self._linearb_service:
                self._linearb_service = LinearBService()
                if not self._linearb_service:
                    raise ValueError("Failed to initialize LinearB service")

            available_metrics = self._linearb_service.get_available_metrics()

            return {
                "available_metrics": available_metrics,
                "timestamp": datetime.now().isoformat(),
                "source": "linearb_pytoolkit",
            }

        except Exception as e:
            self.logger.error(f"Failed to get available metrics: {e}")
            return {
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
                "source": "linearb_pytoolkit",
            }
