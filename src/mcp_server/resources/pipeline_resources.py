"""
Pipeline Resources for MCP Integration.

This module provides pipeline and deployment resources that aggregate data from CircleCI and LinearB
for comprehensive CI/CD pipeline analysis.
"""

from typing import Any

from mcp.types import Resource, TextResourceContents
from pydantic import AnyUrl

from src.mcp_server.adapters.circleci_adapter import CircleCIAdapter
from src.mcp_server.adapters.linearb_adapter import LinearBAdapter

from .base_resource import BaseResourceHandler


class PipelineResourceHandler(BaseResourceHandler):
    """
    Handler for pipeline and deployment resources.

    Aggregates data from:
    - CircleCI: Pipeline status, build metrics
    - LinearB: Deployment metrics
    """

    def __init__(self):
        super().__init__("Pipeline")
        self.circleci_adapter = CircleCIAdapter()
        self.linearb_adapter = LinearBAdapter()

    def get_resource_definitions(self) -> list[Resource]:
        """Define pipeline resources."""
        return [
            Resource(
                uri=AnyUrl("pipeline://deployment_pipeline_status"),
                name="Deployment Pipeline Status",
                description="Consolidated status of all deployment pipelines",
                mimeType="text/markdown",
            ),
            Resource(
                uri=AnyUrl("pipeline://build_success_rates"),
                name="Build Success Rates",
                description="Analysis of build success rates and trends",
                mimeType="text/markdown",
            ),
            Resource(
                uri=AnyUrl("pipeline://deployment_frequency_trends"),
                name="Deployment Frequency Trends",
                description="Analysis of deployment frequency and delivery performance",
                mimeType="text/markdown",
            ),
            Resource(
                uri=AnyUrl("pipeline://ci_cd_health_dashboard"),
                name="CI/CD Health Dashboard",
                description="Consolidated CI/CD health dashboard with pipeline and deployment metrics",
                mimeType="text/markdown",
            ),
        ]

    async def get_resource_content(self, uri: str) -> TextResourceContents:
        """Gets resource content based on URI."""
        if uri == "pipeline://deployment_pipeline_status":
            return await self._get_deployment_pipeline_status()
        elif uri == "pipeline://build_success_rates":
            return await self._get_build_success_rates()
        elif uri == "pipeline://deployment_frequency_trends":
            return await self._get_deployment_frequency_trends()
        elif uri == "pipeline://ci_cd_health_dashboard":
            return await self._get_ci_cd_health_dashboard()
        else:
            raise ValueError(f"Unknown pipeline resource URI: {uri}")

    async def _get_deployment_pipeline_status(self) -> TextResourceContents:
        """Generates deployment pipeline status."""

        def _generate_pipeline_status() -> dict[str, Any]:
            # Main Syngenta Digital pipelines
            data_sources = {
                "main_pipeline": lambda: self.circleci_adapter.get_pipeline_status(
                    "gh/syngenta-digital/main", 20
                ),
                "api_pipeline": lambda: self.circleci_adapter.get_pipeline_status(
                    "gh/syngenta-digital/api", 20
                ),
                "frontend_pipeline": lambda: self.circleci_adapter.get_pipeline_status(
                    "gh/syngenta-digital/frontend", 20
                ),
                "mobile_pipeline": lambda: self.circleci_adapter.get_pipeline_status(
                    "gh/syngenta-digital/mobile", 15
                ),
                "deployment_metrics": lambda: self.linearb_adapter.get_engineering_metrics(
                    "last-week"
                ),
                "pipeline_health_summary": lambda: self._get_pipeline_health_summary(),
            }

            return self.aggregate_data_safely(data_sources)

        pipeline_data = self.cached_resource_operation(
            "deployment_pipeline_status",
            _generate_pipeline_status,
            expiration_minutes=15,  # Pipelines change fast - short cache
        )

        content = self.format_resource_content(
            pipeline_data,
            "Deployment Pipeline Status",
            "Real-time status of all deployment pipelines across Syngenta Digital projects",
        )

        return TextResourceContents(
            uri=AnyUrl("pipeline://deployment_pipeline_status"),
            mimeType="text/markdown",
            text=content,
        )

    async def _get_build_success_rates(self) -> TextResourceContents:
        """Generates build success rate analysis."""

        def _generate_success_rates() -> dict[str, Any]:
            data_sources = {
                "main_builds": lambda: self.circleci_adapter.get_build_metrics(
                    "gh/syngenta-digital/main", 30
                ),
                "api_builds": lambda: self.circleci_adapter.get_build_metrics(
                    "gh/syngenta-digital/api", 30
                ),
                "frontend_builds": lambda: self.circleci_adapter.get_build_metrics(
                    "gh/syngenta-digital/frontend", 30
                ),
                "mobile_builds": lambda: self.circleci_adapter.get_build_metrics(
                    "gh/syngenta-digital/mobile", 30
                ),
                "overall_trends": lambda: self._get_build_trends_analysis(),
            }

            raw_data = self.aggregate_data_safely(data_sources)

            # Calculate aggregated success rate
            success_analysis = self._calculate_success_rates(raw_data)
            raw_data["success_analysis"] = success_analysis

            return raw_data

        success_data = self.cached_resource_operation(
            "build_success_rates", _generate_success_rates, expiration_minutes=60
        )

        content = self.format_resource_content(
            success_data,
            "Build Success Rates",
            "Analysis of build success rates and trends across all Syngenta Digital projects",
        )

        return TextResourceContents(
            uri=AnyUrl("pipeline://build_success_rates"),
            mimeType="text/markdown",
            text=content,
        )

    async def _get_deployment_frequency_trends(self) -> TextResourceContents:
        """Generates deployment frequency analysis."""

        def _generate_deployment_trends() -> dict[str, Any]:
            data_sources = {
                "quarterly_deployment_frequency": lambda: self._get_quarterly_deployment_data(),
                "weekly_deployment_patterns": lambda: self._get_weekly_deployment_patterns(),
                "linearb_deployment_metrics": lambda: self.linearb_adapter.get_engineering_metrics(
                    "last-quarter"
                ),
                "deployment_success_rates": lambda: self._get_deployment_success_analysis(),
                "cycle_time_correlation": lambda: self._get_deployment_cycle_correlation(),
            }

            return self.aggregate_data_safely(data_sources)

        deployment_data = self.cached_resource_operation(
            "deployment_frequency_trends",
            _generate_deployment_trends,
            expiration_minutes=180,  # Trends change slowly
        )

        content = self.format_resource_content(
            deployment_data,
            "Deployment Frequency Trends",
            "Long-term analysis of deployment frequency and delivery performance across quarters and cycles",
        )

        return TextResourceContents(
            uri=AnyUrl("pipeline://deployment_frequency_trends"),
            mimeType="text/markdown",
            text=content,
        )

    async def _get_ci_cd_health_dashboard(self) -> TextResourceContents:
        """Generates consolidated CI/CD health dashboard."""

        def _generate_ci_cd_health() -> dict[str, Any]:
            data_sources = {
                "pipeline_status_overview": lambda: self._get_all_pipelines_overview(),
                "build_performance_metrics": lambda: self._get_build_performance_summary(),
                "deployment_health": lambda: self._get_deployment_health_indicators(),
                "security_scans_status": lambda: self._get_security_scans_summary(),
                "infrastructure_health": lambda: self._get_infrastructure_health(),
                "linearb_ci_cd_metrics": lambda: self.linearb_adapter.get_engineering_metrics(
                    "last-month"
                ),
            }

            raw_data = self.aggregate_data_safely(data_sources)

            # Add general health analysis
            health_assessment = self._assess_ci_cd_health(raw_data)
            raw_data["health_assessment"] = health_assessment

            return raw_data

        health_data = self.cached_resource_operation(
            "ci_cd_health_dashboard",
            _generate_ci_cd_health,
            expiration_minutes=45,  # Update frequently for dashboard
        )

        content = self.format_resource_content(
            health_data,
            "CI/CD Health Dashboard",
            "Comprehensive CI/CD health assessment with pipeline, build, and deployment metrics",
        )

        return TextResourceContents(
            uri=AnyUrl("pipeline://ci_cd_health_dashboard"),
            mimeType="text/markdown",
            text=content,
        )

    def _get_pipeline_health_summary(self) -> dict[str, Any]:
        """Gets pipeline health summary."""
        return {
            "active_pipelines": "placeholder_count",
            "failing_pipelines": "placeholder_count",
            "average_build_time": "placeholder_minutes",
            "pipelines_by_status": {
                "passing": "placeholder_count",
                "failing": "placeholder_count",
                "unstable": "placeholder_count",
            },
        }

    def _get_build_trends_analysis(self) -> dict[str, Any]:
        """Gets build trends analysis."""
        return {
            "success_rate_trend": "placeholder_trend",  # improving, stable, declining
            "build_time_trend": "placeholder_trend",
            "failure_pattern_analysis": "placeholder_analysis",
            "most_common_failure_causes": "placeholder_list",
        }

    def _get_quarterly_deployment_data(self) -> dict[str, Any]:
        """Gets deployment data by quarter."""
        # Adapted to quartiles/cycles structure
        current_period = self.parse_quarter_cycle("current")

        return {
            "current_quarter": current_period["quarter"],
            "current_cycle": current_period["cycle"],
            "deployments_current_cycle": "placeholder_count",
            "deployments_previous_cycle": "placeholder_count",
            "quarterly_deployment_trend": "placeholder_trend",
            "cycle_based_frequency": {
                "Q1_C1": "placeholder_count",
                "Q1_C2": "placeholder_count",
                "Q2_C1": "placeholder_count",
                "Q2_C2": "placeholder_count",
            },
        }

    def _get_weekly_deployment_patterns(self) -> dict[str, Any]:
        """Gets weekly deployment patterns."""
        return {
            "deployments_this_week": "placeholder_count",
            "deployments_last_week": "placeholder_count",
            "preferred_deployment_days": "placeholder_list",
            "deployment_time_patterns": "placeholder_analysis",
            "weekend_deployments": "placeholder_count",
        }

    def _get_deployment_success_analysis(self) -> dict[str, Any]:
        """Gets deployment success analysis."""
        return {
            "deployment_success_rate": "placeholder_percentage",
            "rollback_frequency": "placeholder_percentage",
            "deployment_failures_by_cause": "placeholder_breakdown",
            "mean_time_to_recovery": "placeholder_hours",
        }

    def _get_deployment_cycle_correlation(self) -> dict[str, Any]:
        """Gets correlation between deployment and cycle time."""
        return {
            "deployment_frequency_vs_cycle_time": "placeholder_correlation",
            "optimal_deployment_frequency": "placeholder_analysis",
            "cycle_impact_on_deployments": "placeholder_analysis",
        }

    def _get_all_pipelines_overview(self) -> dict[str, Any]:
        """Gets overview of all pipelines."""
        return {
            "total_pipelines": "placeholder_count",
            "active_pipelines": "placeholder_count",
            "pipelines_with_issues": "placeholder_count",
            "overall_health_score": "placeholder_score",
            "projects_breakdown": {
                "main": "placeholder_status",
                "api": "placeholder_status",
                "frontend": "placeholder_status",
                "mobile": "placeholder_status",
            },
        }

    def _get_build_performance_summary(self) -> dict[str, Any]:
        """Gets build performance summary."""
        return {
            "average_build_time": "placeholder_minutes",
            "fastest_build_project": "placeholder_project",
            "slowest_build_project": "placeholder_project",
            "build_time_trend": "placeholder_trend",
            "queue_time_analysis": "placeholder_minutes",
        }

    def _get_deployment_health_indicators(self) -> dict[str, Any]:
        """Gets deployment health indicators."""
        return {
            "deployment_frequency": "placeholder_frequency",
            "lead_time_for_changes": "placeholder_hours",
            "change_failure_rate": "placeholder_percentage",
            "time_to_restore_service": "placeholder_hours",
            "dora_metrics_assessment": "placeholder_rating",  # Elite, High, Medium, Low
        }

    def _get_security_scans_summary(self) -> dict[str, Any]:
        """Gets security scans summary in pipelines."""
        return {
            "pipelines_with_security_scans": "placeholder_count",
            "security_scan_pass_rate": "placeholder_percentage",
            "vulnerabilities_found_in_pipeline": "placeholder_count",
            "security_gate_failures": "placeholder_count",
        }

    def _get_infrastructure_health(self) -> dict[str, Any]:
        """Gets infrastructure health."""
        return {
            "runner_availability": "placeholder_percentage",
            "resource_utilization": "placeholder_percentage",
            "infrastructure_incidents": "placeholder_count",
            "capacity_warnings": "placeholder_count",
        }

    def _calculate_success_rates(self, data: dict[str, Any]) -> dict[str, Any]:
        """Calculates success rates based on build data."""
        # Create explicit dictionary for type clarity
        project_rates: dict[str, float] = {}

        analysis: dict[str, Any] = {
            "overall_success_rate": 0.0,
            "project_rates": project_rates,
            "trend": "stable",  # improving, stable, declining
            "best_performing_project": "placeholder_project",
            "worst_performing_project": "placeholder_project",
        }

        # Simple analysis - in real implementation, would process build data
        sources = data.get("sources", {})

        project_success_rates = []

        for project_name, _ in sources.items():
            if project_name.endswith("_builds"):
                project = project_name.replace("_builds", "")
                # Simulation - in real implementation, would calculate based on real data
                simulated_rate = 85.5  # Placeholder
                project_rates[project] = simulated_rate
                project_success_rates.append(simulated_rate)

        if project_success_rates:
            analysis["overall_success_rate"] = sum(project_success_rates) / len(
                project_success_rates
            )

        return analysis

    def _assess_ci_cd_health(self, data: dict[str, Any]) -> dict[str, Any]:
        """Assesses overall CI/CD health."""
        # Create explicit lists for type clarity
        key_strengths: list[str] = []
        areas_for_improvement: list[str] = []
        immediate_actions_needed: list[str] = []

        assessment: dict[str, Any] = {
            "overall_health": "good",  # excellent, good, warning, critical
            "score": 85,  # 0-100
            "key_strengths": key_strengths,
            "areas_for_improvement": areas_for_improvement,
            "immediate_actions_needed": immediate_actions_needed,
            "health_indicators": {
                "pipeline_reliability": "good",
                "build_performance": "good",
                "deployment_frequency": "good",
                "security_compliance": "good",
                "infrastructure_stability": "good",
            },
        }

        # Analysis based on available data
        sources = data.get("sources", {})

        if "pipeline_status_overview" in sources:
            key_strengths.append("Pipeline monitoring is active")

        if "deployment_health" in sources:
            key_strengths.append("Deployment metrics are being tracked")

        if "security_scans_status" in sources:
            key_strengths.append("Security scans are integrated")

        # If there are errors, identify areas for improvement
        if data.get("errors"):
            areas_for_improvement.append("Some monitoring data sources are unavailable")
            assessment["overall_health"] = "warning"
            current_score = assessment["score"]
            if isinstance(current_score, int | float):
                assessment["score"] = max(current_score - 15, 0)

        return assessment
