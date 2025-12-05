"""SonarQube Adapter for PyToolkit MCP Integration.

This adapter reuses the existing PyToolkit SonarQube service, providing
an MCP-compatible interface for code quality operations.
"""

from datetime import datetime
from typing import Any

# Reusing existing PyToolkit SonarQube service
from domains.syngenta.sonarqube.sonarqube_service import SonarQubeService

from .base_adapter import BaseAdapter


class SonarQubeAdapter(BaseAdapter):
    """SonarQube adapter that reuses the existing PyToolkit SonarQube service.

    Features:
    - 27 predefined Syngenta Digital projects
    - 16 comprehensive quality metrics
    - Batch operations for multiple projects
    - 1-hour intelligent caching
    - Quality gate analysis
    """

    # Default quality metrics used by PyToolkit
    DEFAULT_METRICS = [
        "alert_status",  # Quality gate status
        "bugs",  # Number of bugs
        "reliability_rating",  # Reliability rating (A-E)
        "vulnerabilities",  # Number of vulnerabilities
        "security_rating",  # Security rating (A-E)
        "security_hotspots_reviewed",  # Security hotspots review %
        "security_review_rating",  # Security review rating (A-E)
        "code_smells",  # Number of code smells
        "sqale_rating",  # Maintainability rating (A-E)
        "duplicated_lines_density",  # Duplicated lines %
        "coverage",  # Test coverage %
        "ncloc",  # Non-commented lines of code
        "ncloc_language_distribution",  # Language distribution
        "security_issues",  # Security issues count
        "reliability_issues",  # Reliability issues count
        "maintainability_issues",  # Maintainability issues count
    ]

    def __init__(self) -> None:
        super().__init__("SonarQube")

        # Ensure SonarQube-specific environment is loaded
        from utils.env_loader import ensure_sonarqube_env_loaded

        ensure_sonarqube_env_loaded()

        # Initialize service (lazy loading)
        self._sonarqube_service: SonarQubeService | None = None

    def initialize_service(self) -> Any:
        """Initialize SonarQube service from PyToolkit."""
        try:
            # Let the service use environment variables for configuration
            self._sonarqube_service = SonarQubeService()

            self.logger.info("SonarQube service initialized successfully")
            return self._sonarqube_service

        except Exception as e:
            self.logger.error(f"Failed to initialize SonarQube service: {e}")
            raise

    @property
    def sonarqube_service(self):
        """Access to SonarQube service."""
        if self._sonarqube_service is None:
            _ = self.service  # Trigger lazy loading
        assert self._sonarqube_service is not None
        return self._sonarqube_service

    def get_all_projects_with_metrics(
        self,
        organization: str | None = None,
        project_keys: list[str] | None = None,
        custom_metrics: list[str] | None = None,
    ) -> dict[str, Any]:
        """Get all predefined projects with their quality metrics.

        Args:
            organization: SonarQube organization
            project_keys: Filter by specific project keys
            custom_metrics: Custom metric keys (uses defaults if None)

        Returns:
            Projects with comprehensive quality metrics
        """

        def _fetch_projects_with_metrics(**kwargs) -> dict[str, Any]:
            metrics = kwargs.get("custom_metrics") or self.DEFAULT_METRICS

            result = self.sonarqube_service.get_projects_by_list(
                organization=kwargs.get("organization"),
                include_measures=True,
                metric_keys=metrics,
                filter_project_keys=kwargs.get("project_keys"),
            )

            # Add summary statistics
            projects = result.get("projects", [])

            summary = {
                "total_projects": len(projects),
                "projects_with_quality_gate": len(
                    [p for p in projects if p.get("measures", {}).get("alert_status", {}).get("value") == "OK"]
                ),
                "projects_analyzed": len([p for p in projects if p.get("lastAnalysisDate")]),
                "analysis_date": datetime.now().isoformat(),
            }

            # Calculate quality distributions
            quality_distribution = {
                "quality_gates": {"OK": 0, "ERROR": 0, "NONE": 0},
                "security_ratings": {"A": 0, "B": 0, "C": 0, "D": 0, "E": 0},
                "reliability_ratings": {"A": 0, "B": 0, "C": 0, "D": 0, "E": 0},
                "maintainability_ratings": {"A": 0, "B": 0, "C": 0, "D": 0, "E": 0},
            }

            for project in projects:
                measures = project.get("measures", {})

                # Quality gate distribution
                qg_status = measures.get("alert_status", {}).get("value", "NONE")
                if qg_status in quality_distribution["quality_gates"]:
                    quality_distribution["quality_gates"][qg_status] += 1

                # Security rating distribution
                sec_rating = measures.get("security_rating", {}).get("value")
                if sec_rating and sec_rating in quality_distribution["security_ratings"]:
                    quality_distribution["security_ratings"][sec_rating] += 1

                # Reliability rating distribution
                rel_rating = measures.get("reliability_rating", {}).get("value")
                if rel_rating and rel_rating in quality_distribution["reliability_ratings"]:
                    quality_distribution["reliability_ratings"][rel_rating] += 1

                # Maintainability rating distribution
                maint_rating = measures.get("sqale_rating", {}).get("value")
                if maint_rating and maint_rating in quality_distribution["maintainability_ratings"]:
                    quality_distribution["maintainability_ratings"][maint_rating] += 1

            result.update({"summary": summary, "quality_distribution": quality_distribution})

            return result

        return self.cached_operation(
            "projects_with_metrics",
            _fetch_projects_with_metrics,
            expiration_minutes=60,  # 1-hour cache as per PyToolkit standard
            organization=organization,
            project_keys=project_keys,
            custom_metrics=custom_metrics,
        )

    def get_project_details(
        self,
        project_key: str,
        include_issues: bool = False,
        custom_metrics: list[str] | None = None,
    ) -> dict[str, Any]:
        """Get detailed information for a specific project.

        Args:
            project_key: SonarQube project key
            include_issues: Include project issues in response
            custom_metrics: Custom metric keys

        Returns:
            Detailed project information with metrics and optionally issues
        """

        def _fetch_project_details(**kwargs) -> dict[str, Any]:
            metrics = kwargs.get("custom_metrics") or self.DEFAULT_METRICS
            project_key = kwargs["project_key"]

            # Get project measures
            measures_data = self.sonarqube_service.get_project_measures(project_key=project_key, metric_keys=metrics)

            result = {
                "project_key": project_key,
                "measures": measures_data,
                "generated_at": datetime.now().isoformat(),
            }

            # Include issues if requested
            if kwargs.get("include_issues"):
                issues = self.sonarqube_service.get_project_issues(project_key=project_key)

                # Summarize issues by severity
                issue_summary: dict[str, int] = {}
                for issue in issues:
                    severity = issue.get("severity", "UNKNOWN")
                    issue_summary[severity] = issue_summary.get(severity, 0) + 1

                result.update(
                    {
                        "issues": {
                            "total": len(issues),
                            "by_severity": issue_summary,
                            "details": issues,
                        }
                    }
                )

            return result

        return self.cached_operation(
            "project_details",
            _fetch_project_details,
            expiration_minutes=60,
            project_key=project_key,
            include_issues=include_issues,
            custom_metrics=custom_metrics,
        )

    def get_quality_dashboard(
        self,
        organization: str | None = None,
        focus_projects: list[str] | None = None,
    ) -> dict[str, Any]:
        """Get a comprehensive quality dashboard.

        Args:
            organization: SonarQube organization
            focus_projects: Specific projects to highlight

        Returns:
            Quality dashboard with key insights
        """

        def _fetch_quality_dashboard(**kwargs) -> dict[str, Any]:
            # Get all projects with metrics
            projects_data = self.get_all_projects_with_metrics(
                organization=kwargs.get("organization"),
                project_keys=kwargs.get("focus_projects"),
            )

            projects = projects_data.get("projects", [])

            # Calculate key insights
            insights: dict[str, list[dict[str, Any]]] = {
                "critical_projects": [],
                "top_security_risks": [],
                "technical_debt_leaders": [],
                "coverage_champions": [],
            }

            for project in projects:
                measures = project.get("measures", {})
                project_key = project.get("key")
                project_name = project.get("name")

                # Critical projects (quality gate failed)
                if measures.get("alert_status", {}).get("value") == "ERROR":
                    insights["critical_projects"].append(
                        {
                            "key": project_key,
                            "name": project_name,
                            "quality_gate": "FAILED",
                        }
                    )

                # Security risks (D or E rating)
                sec_rating = measures.get("security_rating", {}).get("value")
                if sec_rating in ["D", "E"]:
                    insights["top_security_risks"].append(
                        {
                            "key": project_key,
                            "name": project_name,
                            "security_rating": sec_rating,
                            "vulnerabilities": measures.get("vulnerabilities", {}).get("value", 0),
                        }
                    )

                # Technical debt (high maintainability issues)
                maint_issues = int(measures.get("maintainability_issues", {}).get("value", 0))
                if maint_issues > 100:  # Threshold for high technical debt
                    insights["technical_debt_leaders"].append(
                        {
                            "key": project_key,
                            "name": project_name,
                            "maintainability_issues": maint_issues,
                            "rating": measures.get("sqale_rating", {}).get("value"),
                        }
                    )

                # Coverage champions (>80% coverage)
                coverage = float(measures.get("coverage", {}).get("value", 0))
                if coverage > 80:
                    insights["coverage_champions"].append(
                        {"key": project_key, "name": project_name, "coverage": coverage}
                    )

            # Sort insights by severity/importance
            insights["top_security_risks"].sort(key=lambda x: x.get("vulnerabilities", 0), reverse=True)
            insights["technical_debt_leaders"].sort(key=lambda x: x.get("maintainability_issues", 0), reverse=True)
            insights["coverage_champions"].sort(key=lambda x: x.get("coverage", 0), reverse=True)

            return {
                "dashboard": {
                    "generated_at": datetime.now().isoformat(),
                    "organization": kwargs.get("organization"),
                    "focus_projects": kwargs.get("focus_projects"),
                    "total_projects_analyzed": len(projects),
                },
                "insights": insights,
                "summary": projects_data.get("summary", {}),
                "quality_distribution": projects_data.get("quality_distribution", {}),
            }

        return self.cached_operation(
            "quality_dashboard",
            _fetch_quality_dashboard,
            expiration_minutes=30,  # Shorter cache for dashboard
            organization=organization,
            focus_projects=focus_projects,
        )

    def get_available_metrics(self) -> dict[str, Any]:
        """Get all available SonarQube metrics.

        Returns:
            list of available metrics with descriptions
        """

        def _fetch_available_metrics() -> dict[str, Any]:
            metrics = self.sonarqube_service.get_available_metrics()

            return {
                "total_metrics": len(metrics),
                "default_metrics": self.DEFAULT_METRICS,
                "all_metrics": metrics,
                "generated_at": datetime.now().isoformat(),
            }

        return self.cached_operation(
            "available_metrics",
            _fetch_available_metrics,
            expiration_minutes=240,  # 4-hour cache for static data
        )

    def health_check(self) -> dict[str, Any]:
        """Enhanced health check for SonarQube adapter."""
        base_health = super().health_check()

        try:
            # Test SonarQube connectivity
            service = self.sonarqube_service

            # Try a simple operation - get available metrics
            metrics = service.get_available_metrics()

            base_health.update(
                {
                    "sonarqube_connectivity": "healthy",
                    "test_operation": "get_available_metrics",
                    "metrics_available": len(metrics),
                    "default_metrics_count": len(self.DEFAULT_METRICS),
                    "cache_expiration_minutes": 60,
                }
            )

        except Exception as e:
            base_health.update({"sonarqube_connectivity": "unhealthy", "connectivity_error": str(e)})

        return base_health
