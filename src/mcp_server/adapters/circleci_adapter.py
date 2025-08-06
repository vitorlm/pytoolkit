"""
CircleCI Adapter for PyToolkit MCP Integration.

This adapter provides full CircleCI integration using the existing
PyToolkit CircleCI infrastructure for pipeline analytics and monitoring.
"""

import os
import sys
from datetime import datetime
from typing import Any


# Add src to path to import PyToolkit domains
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from domains.circleci.circleci_service import CircleCIService
from domains.circleci.pipeline_details_service import PipelineDetailsService
from utils.env_loader import ensure_circleci_env_loaded

from .base_adapter import BaseAdapter


class CircleCIAdapter(BaseAdapter):
    """
    CircleCI adapter for CI/CD pipeline analytics.

    This adapter uses the existing CircleCI infrastructure from PyToolkit
    to provide pipeline status, build metrics, and performance analysis.
    """

    def __init__(self) -> None:
        super().__init__("CircleCI")
        # Load CircleCI environment variables
        ensure_circleci_env_loaded()

        # Initialize services
        self._circleci_service: CircleCIService | None = None
        self._pipeline_service: PipelineDetailsService | None = None
        self._project_slug: str | None = None

    def initialize_service(self) -> Any:
        """Initialize CircleCI service using PyToolkit infrastructure."""
        try:
            # Get required environment variables
            token = os.getenv("CIRCLECI_TOKEN")
            project_slug = os.getenv("CIRCLECI_PROJECT_SLUG", "")

            if not token:
                self.logger.warning(
                    "CIRCLECI_TOKEN not configured - CircleCI features will be unavailable"
                )
                return {
                    "initialized": False,
                    "service_type": "CircleCI API",
                    "status": "not_configured",
                    "error": "CIRCLECI_TOKEN environment variable is required",
                    "initialized_at": datetime.now().isoformat(),
                }

            # Initialize CircleCI service
            self._circleci_service = CircleCIService(
                token=token, project_slug=project_slug
            )
            self._project_slug = project_slug

            # Initialize pipeline details service
            if project_slug:
                self._pipeline_service = PipelineDetailsService(
                    token=token, project_slug=project_slug
                )

            service_info = {
                "initialized": True,
                "service_type": "CircleCI API",
                "version": "pytoolkit-integrated",
                "project_slug": project_slug,
                "has_pipeline_service": self._pipeline_service is not None,
                "initialized_at": datetime.now().isoformat(),
            }

            self.logger.info("CircleCI service initialized successfully with PyToolkit")
            return service_info

        except Exception as e:
            self.logger.error(f"Failed to initialize CircleCI service: {e}")
            raise

    def get_pipeline_status(
        self, project: str | None = None, limit: int = 10
    ) -> dict[str, Any]:
        """
        Get pipeline status for a project.

        Args:
            project: CircleCI project identifier (optional, uses default if not provided)
            limit: Number of pipelines to fetch

        Returns:
            Pipeline status information
        """

        def _fetch_pipeline_status(**kwargs) -> dict[str, Any]:
            try:
                if not self._circleci_service:
                    self.initialize_service()

                # Use provided project or fallback to configured project
                target_project = kwargs.get("project") or self._project_slug
                if target_project and target_project != self._project_slug:
                    # Create new service instance for different project
                    token = os.getenv("CIRCLECI_TOKEN")
                    if not token:
                        raise ValueError(
                            "CIRCLECI_TOKEN environment variable is required"
                        )
                    temp_service = CircleCIService(
                        token=token, project_slug=target_project
                    )
                    pipelines = temp_service.export_pipelines(limit=kwargs["limit"])
                else:
                    if not self._circleci_service:
                        raise ValueError("CircleCI service not initialized")
                    pipelines = self._circleci_service.export_pipelines(
                        limit=kwargs["limit"]
                    )

                # Calculate basic statistics
                total_pipelines = len(pipelines)
                successful = len([p for p in pipelines if p.get("state") == "success"])
                failed = len([p for p in pipelines if p.get("state") == "failed"])
                success_rate = (
                    (successful / total_pipelines * 100) if total_pipelines > 0 else 0
                )

                last_build = pipelines[0] if pipelines else None

                return {
                    "project": target_project,
                    "limit": kwargs["limit"],
                    "status": "active",
                    "last_build": {
                        "number": last_build.get("number") if last_build else None,
                        "state": last_build.get("state") if last_build else None,
                        "created_at": (
                            last_build.get("created_at") if last_build else None
                        ),
                    },
                    "success_rate": round(success_rate, 2),
                    "pipelines_fetched": total_pipelines,
                    "successful_pipelines": successful,
                    "failed_pipelines": failed,
                    "generated_at": datetime.now().isoformat(),
                    "source": "circleci_pytoolkit",
                }

            except Exception as e:
                self.logger.error(f"Failed to fetch pipeline status: {e}")
                return {
                    "error": str(e),
                    "project": kwargs.get("project"),
                    "timestamp": datetime.now().isoformat(),
                    "source": "circleci_pytoolkit",
                }

        return self.cached_operation(
            "pipeline_status",
            _fetch_pipeline_status,
            expiration_minutes=30,
            project=project,
            limit=limit,
        )

    def get_build_metrics(
        self, project: str | None = None, limit: int = 100
    ) -> dict[str, Any]:
        """
        Get build metrics for a project.

        Args:
            project: CircleCI project identifier (optional, uses default if not provided)
            limit: Number of builds to analyze for metrics

        Returns:
            Build metrics information
        """

        def _fetch_build_metrics(**kwargs) -> dict[str, Any]:
            try:
                if not self._circleci_service:
                    self.initialize_service()

                # Use provided project or fallback to configured project
                target_project = kwargs.get("project") or self._project_slug
                if target_project and target_project != self._project_slug:
                    # Create new service instance for different project
                    token = os.getenv("CIRCLECI_TOKEN")
                    if not token:
                        raise ValueError(
                            "CIRCLECI_TOKEN environment variable is required"
                        )
                    temp_service = CircleCIService(
                        token=token, project_slug=target_project
                    )
                    analysis_result = temp_service.run_complete_analysis()
                else:
                    if not self._circleci_service:
                        raise ValueError("CircleCI service not initialized")
                    analysis_result = self._circleci_service.run_complete_analysis()

                return {
                    "project": target_project,
                    "limit": kwargs["limit"],
                    "summary": analysis_result.get("summary", {}),
                    "bottlenecks": analysis_result.get("bottlenecks", []),
                    "optimization_plan": analysis_result.get("optimization_plan", {}),
                    "output_dir": analysis_result.get("output_dir"),
                    "generated_at": datetime.now().isoformat(),
                    "source": "circleci_pytoolkit",
                }

            except Exception as e:
                self.logger.error(f"Failed to fetch build metrics: {e}")
                return {
                    "error": str(e),
                    "project": kwargs.get("project"),
                    "timestamp": datetime.now().isoformat(),
                    "source": "circleci_pytoolkit",
                }

        return self.cached_operation(
            "build_metrics",
            _fetch_build_metrics,
            expiration_minutes=60,
            project=project,
            limit=limit,
        )

    def health_check(self) -> dict[str, Any]:
        """Health check for CircleCI adapter."""
        base_health = super().health_check()

        try:
            # Test basic connectivity
            if not self._circleci_service:
                self.initialize_service()

            # Try to list projects to verify connectivity
            if not self._circleci_service:
                raise ValueError("CircleCI service not initialized")
            projects = self._circleci_service.list_projects()
            connectivity_status = "healthy" if projects else "limited"

            base_health.update(
                {
                    "circleci_connectivity": connectivity_status,
                    "implementation_status": "active",
                    "project_slug": self._project_slug,
                    "projects_accessible": len(projects),
                    "has_pipeline_service": self._pipeline_service is not None,
                    "note": "Full CircleCI integration using PyToolkit infrastructure",
                }
            )

        except Exception as e:
            base_health.update(
                {
                    "circleci_connectivity": "error",
                    "implementation_status": "error",
                    "error": str(e),
                    "note": "CircleCI integration error",
                }
            )

        return base_health

    def list_projects(self) -> dict[str, Any]:
        """
        List all accessible CircleCI projects.

        Returns:
            dict with projects information
        """

        def _list_projects(**kwargs) -> dict[str, Any]:
            try:
                if not self._circleci_service:
                    self.initialize_service()

                if not self._circleci_service:
                    raise ValueError("CircleCI service not initialized")
                projects = self._circleci_service.list_projects()

                return {
                    "projects": projects,
                    "total_projects": len(projects),
                    "timestamp": datetime.now().isoformat(),
                    "source": "circleci_pytoolkit",
                }

            except Exception as e:
                self.logger.error(f"Failed to list projects: {e}")
                return {
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                    "source": "circleci_pytoolkit",
                }

        return self.cached_operation(
            "list_projects",
            _list_projects,
            expiration_minutes=120,  # Projects list changes infrequently
        )

    def analyze_deployment_frequency(
        self, project: str | None = None, days: int = 30
    ) -> dict[str, Any]:
        """
        Analyze deployment frequency for a project.

        Args:
            project: CircleCI project identifier (optional, uses default if not provided)
            days: Number of days to analyze

        Returns:
            dict with deployment frequency analysis
        """

        def _analyze_deployment_frequency(**kwargs) -> dict[str, Any]:
            try:
                if not self._pipeline_service and not self._circleci_service:
                    self.initialize_service()

                target_project = kwargs.get("project") or self._project_slug

                # Get pipeline data for analysis
                if target_project and target_project != self._project_slug:
                    token = os.getenv("CIRCLECI_TOKEN")
                    if not token:
                        raise ValueError(
                            "CIRCLECI_TOKEN environment variable is required"
                        )
                    temp_service = CircleCIService(
                        token=token, project_slug=target_project
                    )
                    pipelines = temp_service.export_pipelines(
                        limit=kwargs.get("days", 30) * 5
                    )  # Rough estimation
                else:
                    if not self._circleci_service:
                        raise ValueError("CircleCI service not initialized")
                    pipelines = self._circleci_service.export_pipelines(
                        limit=kwargs.get("days", 30) * 5
                    )

                # Analyze deployment patterns
                successful_pipelines = [
                    p for p in pipelines if p.get("state") == "success"
                ]

                # Calculate deployment frequency
                deployment_count = len(successful_pipelines)
                days_analyzed = kwargs.get("days", 30)
                frequency_per_day = (
                    deployment_count / days_analyzed if days_analyzed > 0 else 0
                )

                return {
                    "project": target_project,
                    "days_analyzed": days_analyzed,
                    "total_deployments": deployment_count,
                    "frequency_per_day": round(frequency_per_day, 2),
                    "frequency_per_week": round(frequency_per_day * 7, 2),
                    "successful_pipelines": len(successful_pipelines),
                    "total_pipelines": len(pipelines),
                    "success_rate": (
                        round((len(successful_pipelines) / len(pipelines) * 100), 2)
                        if pipelines
                        else 0
                    ),
                    "timestamp": datetime.now().isoformat(),
                    "source": "circleci_pytoolkit",
                }

            except Exception as e:
                self.logger.error(f"Failed to analyze deployment frequency: {e}")
                return {
                    "error": str(e),
                    "project": kwargs.get("project"),
                    "timestamp": datetime.now().isoformat(),
                    "source": "circleci_pytoolkit",
                }

        return self.cached_operation(
            "deployment_frequency",
            _analyze_deployment_frequency,
            expiration_minutes=120,
            project=project,
            days=days,
        )
