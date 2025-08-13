"""
Quality Metrics Resources for MCP Integration.

This module provides quality metrics resources that aggregate data from SonarQube and CircleCI
for comprehensive code quality analysis.
"""

from typing import Any

from mcp.types import Resource, TextResourceContents
from pydantic import AnyUrl

from ..adapters.circleci_adapter import CircleCIAdapter
from ..adapters.sonarqube_adapter import SonarQubeAdapter

from .base_resource import BaseResourceHandler


class QualityMetricsResourceHandler(BaseResourceHandler):
    """
    Handler for quality metrics resources.

    Aggregates data from:
    - SonarQube: Qualidade de código, issues, quality gates
    - CircleCI: Build success, pipeline health
    """

    def __init__(self):
        super().__init__("QualityMetrics")
        self.sonarqube_adapter = SonarQubeAdapter()
        self.circleci_adapter = CircleCIAdapter()

    def get_resource_definitions(self) -> list[Resource]:
        """Define quality metrics resources."""
        return [
            Resource(
                uri=AnyUrl("quality://code_quality_overview"),
                name="Code Quality Overview",
                description="Visão consolidada de qualidade de código via SonarQube + CircleCI",
                mimeType="text/markdown",
            ),
            Resource(
                uri=AnyUrl("quality://technical_debt_analysis"),
                name="Technical Debt Analysis",
                description="Technical debt analysis with data-based prioritization",
                mimeType="text/markdown",
            ),
            Resource(
                uri=AnyUrl("quality://security_vulnerabilities_summary"),
                name="Security Vulnerabilities Summary",
                description="Relatório consolidado de vulnerabilidades de segurança",
                mimeType="text/markdown",
            ),
            Resource(
                uri=AnyUrl("quality://weekly_quality_health"),
                name="Weekly Quality Health Report",
                description="Relatório semanal de saúde de qualidade compatível com run_reports.sh",
                mimeType="text/markdown",
            ),
        ]

    async def get_resource_content(self, uri: str) -> TextResourceContents:
        """Gets resource content based on URI."""
        if uri == "quality://code_quality_overview":
            return await self._get_code_quality_overview()
        elif uri == "quality://technical_debt_analysis":
            return await self._get_technical_debt_analysis()
        elif uri == "quality://security_vulnerabilities_summary":
            return await self._get_security_vulnerabilities_summary()
        elif uri == "quality://weekly_quality_health":
            return await self._get_weekly_quality_health()
        else:
            raise ValueError(f"Unknown quality resource URI: {uri}")

    async def _get_code_quality_overview(self) -> TextResourceContents:
        """Generates code quality overview."""

        def _generate_quality_overview() -> dict[str, Any]:
            data_sources = {
                "sonarqube_overview": lambda: self.sonarqube_adapter.get_all_projects_with_metrics(),
                "sonarqube_syngenta_projects": lambda: self._get_syngenta_projects_summary(),
                "pipeline_health_main": lambda: self.circleci_adapter.get_build_metrics(
                    "gh/syngenta-digital/main-project", 30
                ),
                "overall_quality_gates": lambda: self._get_quality_gates_summary(),
            }

            return self.aggregate_data_safely(
                data_sources, required_sources=["sonarqube_overview"]
            )

        quality_data = self.cached_resource_operation(
            "code_quality_overview",
            _generate_quality_overview,
            expiration_minutes=90,  # Cache por 1.5 horas
        )

        content = self.format_resource_content(
            quality_data,
            "Code Quality Overview",
            "Consolidated code quality metrics from SonarQube and CI/CD pipelines",
        )

        return TextResourceContents(
            uri=AnyUrl("quality://code_quality_overview"),
            mimeType="text/markdown",
            text=content,
        )

    async def _get_technical_debt_analysis(self) -> TextResourceContents:
        """Generates technical debt analysis."""

        def _generate_debt_analysis() -> dict[str, Any]:
            # Search for issues of multiple types from SonarQube
            data_sources = {
                "code_smells_critical": lambda: self._get_critical_code_smells(),
                "bugs_by_severity": lambda: self._get_bugs_by_severity(),
                "quality_metrics_summary": lambda: self.sonarqube_adapter.get_all_projects_with_metrics(),
                "maintainability_rating": lambda: self._get_maintainability_summary(),
                "coverage_analysis": lambda: self._get_coverage_analysis(),
            }

            raw_data = self.aggregate_data_safely(data_sources)

            # Add prioritization analysis
            debt_analysis = self._analyze_technical_debt(raw_data)
            raw_data["debt_analysis"] = debt_analysis

            return raw_data

        debt_data = self.cached_resource_operation(
            "technical_debt_analysis",
            _generate_debt_analysis,
            expiration_minutes=240,  # Cache for 4 hours (heavy analysis)
        )

        content = self.format_resource_content(
            debt_data,
            "Technical Debt Analysis",
            "Prioritized technical debt analysis with actionable recommendations",
        )

        return TextResourceContents(
            uri=AnyUrl("quality://technical_debt_analysis"),
            mimeType="text/markdown",
            text=content,
        )

    async def _get_security_vulnerabilities_summary(self) -> TextResourceContents:
        """Generates security vulnerabilities report."""

        def _generate_security_summary() -> dict[str, Any]:
            data_sources = {
                "vulnerabilities_by_severity": lambda: self._get_vulnerabilities_by_severity(),
                "security_hotspots": lambda: self._get_security_hotspots_summary(),
                "security_rating_overview": lambda: self._get_security_rating_overview(),
                "pipeline_security_checks": lambda: self._get_pipeline_security_status(),
            }

            return self.aggregate_data_safely(
                data_sources, required_sources=["vulnerabilities_by_severity"]
            )

        security_data = self.cached_resource_operation(
            "security_vulnerabilities_summary",
            _generate_security_summary,
            expiration_minutes=60,  # Security needs to be more current
        )

        content = self.format_resource_content(
            security_data,
            "Security Vulnerabilities Summary",
            "Comprehensive security analysis across all monitored projects",
        )

        return TextResourceContents(
            uri=AnyUrl("quality://security_vulnerabilities_summary"),
            mimeType="text/markdown",
            text=content,
        )

    async def _get_weekly_quality_health(self) -> TextResourceContents:
        """Generates weekly quality health report."""

        def _generate_weekly_quality() -> dict[str, Any]:
            data_sources = {
                "sonarqube_weekly_snapshot": lambda: self.sonarqube_adapter.get_all_projects_with_metrics(),
                "quality_gates_status": lambda: self._get_quality_gates_summary(),
                "critical_issues_count": lambda: self._get_critical_issues_weekly(),
                "coverage_trends": lambda: self._get_coverage_trends_weekly(),
                "pipeline_success_rates": lambda: self._get_pipeline_success_weekly(),
            }

            weekly_data = self.aggregate_data_safely(
                data_sources, required_sources=["sonarqube_weekly_snapshot"]
            )

            # Add metadata compatible with report_template.md
            weekly_data["weekly_report_metadata"] = {
                "template_section": "SonarCloud – Quality & Security Health",
                "compatible_with_run_reports": True,
                "key_metrics": [
                    "Quality Gate Status",
                    "Coverage Percentage",
                    "Bugs Count",
                    "Reliability Rating",
                    "Code Smells Count",
                    "Security Hotspots Reviewed",
                ],
            }

            return weekly_data

        weekly_data = self.cached_resource_operation(
            "weekly_quality_health",
            _generate_weekly_quality,
            expiration_minutes=60,  # Cache for 1 hour for weekly data
        )

        content = self.format_resource_content(
            weekly_data,
            "Weekly Quality Health Report",
            "Weekly code quality and security health metrics compatible with report_template.md format",
        )

        return TextResourceContents(
            uri=AnyUrl("quality://weekly_quality_health"),
            mimeType="text/markdown",
            text=content,
        )

    def _get_syngenta_projects_summary(self) -> dict[str, Any]:
        """Gets Syngenta projects summary."""
        # Based on SonarQube adapter projects list
        return {
            "total_projects": "placeholder_count",
            "projects_with_quality_gate_passed": "placeholder_count",
            "projects_with_quality_gate_failed": "placeholder_count",
            "average_coverage": "placeholder_percentage",
            "note": "Data would come from SonarQube projects list with measures",
        }

    def _get_quality_gates_summary(self) -> dict[str, Any]:
        """Gets quality gates summary."""
        return {
            "passed": "placeholder_count",
            "failed": "placeholder_count",
            "error": "placeholder_count",
            "projects_by_status": "placeholder_breakdown",
            "data_source": "SonarQube list-projects with quality gate status",
        }

    def _get_critical_code_smells(self) -> dict[str, Any]:
        """Gets critical code smells."""
        return {
            "critical_count": "placeholder_count",
            "major_count": "placeholder_count",
            "projects_most_affected": "placeholder_list",
            "note": "Data would come from SonarQube issues API with CODE_SMELL type",
        }

    def _get_bugs_by_severity(self) -> dict[str, Any]:
        """Gets bugs by severity."""
        return {
            "blocker": "placeholder_count",
            "critical": "placeholder_count",
            "major": "placeholder_count",
            "minor": "placeholder_count",
            "info": "placeholder_count",
            "total": "placeholder_count",
        }

    def _get_maintainability_summary(self) -> dict[str, Any]:
        """Gets maintainability summary."""
        return {
            "rating_A": "placeholder_count",
            "rating_B": "placeholder_count",
            "rating_C": "placeholder_count",
            "rating_D": "placeholder_count",
            "rating_E": "placeholder_count",
            "average_technical_debt": "placeholder_hours",
        }

    def _get_coverage_analysis(self) -> dict[str, Any]:
        """Gets coverage analysis."""
        return {
            "average_coverage": "placeholder_percentage",
            "projects_above_80": "placeholder_count",
            "projects_below_60": "placeholder_count",
            "worst_coverage_projects": "placeholder_list",
        }

    def _get_vulnerabilities_by_severity(self) -> dict[str, Any]:
        """Gets vulnerabilities by severity."""
        return {
            "blocker": "placeholder_count",
            "critical": "placeholder_count",
            "major": "placeholder_count",
            "minor": "placeholder_count",
            "info": "placeholder_count",
            "projects_most_vulnerable": "placeholder_list",
        }

    def _get_security_hotspots_summary(self) -> dict[str, Any]:
        """Gets security hotspots summary."""
        return {
            "total_hotspots": "placeholder_count",
            "reviewed_percentage": "placeholder_percentage",
            "safe_hotspots": "placeholder_count",
            "to_review": "placeholder_count",
        }

    def _get_security_rating_overview(self) -> dict[str, Any]:
        """Gets security rating overview."""
        return {
            "rating_A": "placeholder_count",
            "rating_B": "placeholder_count",
            "rating_C": "placeholder_count",
            "rating_D": "placeholder_count",
            "rating_E": "placeholder_count",
            "projects_needing_attention": "placeholder_list",
        }

    def _get_pipeline_security_status(self) -> dict[str, Any]:
        """Gets pipeline security status."""
        return {
            "pipelines_with_security_scans": "placeholder_count",
            "pipelines_failing_security": "placeholder_count",
            "average_scan_time": "placeholder_minutes",
            "note": "Data would come from CircleCI build metrics with security step analysis",
        }

    def _get_critical_issues_weekly(self) -> dict[str, Any]:
        """Gets weekly critical issues."""
        return {
            "new_critical_bugs": "placeholder_count",
            "new_vulnerabilities": "placeholder_count",
            "resolved_critical": "placeholder_count",
            "net_change": "placeholder_change",
        }

    def _get_coverage_trends_weekly(self) -> dict[str, Any]:
        """Gets weekly coverage trends."""
        return {
            "current_week_average": "placeholder_percentage",
            "previous_week_average": "placeholder_percentage",
            "trend": "placeholder_direction",
            "projects_improved": "placeholder_count",
            "projects_declined": "placeholder_count",
        }

    def _get_pipeline_success_weekly(self) -> dict[str, Any]:
        """Gets weekly pipeline success rate."""
        return {
            "success_rate_current": "placeholder_percentage",
            "success_rate_previous": "placeholder_percentage",
            "builds_total": "placeholder_count",
            "builds_failed": "placeholder_count",
            "main_pipeline_health": "placeholder_status",
        }

    def _analyze_technical_debt(self, data: dict[str, Any]) -> dict[str, Any]:
        """Analyzes technical debt and generates recommendations."""
        # Create explicit lists for type clarity
        critical_issues: list[str] = []
        high_issues: list[str] = []
        medium_issues: list[str] = []
        low_issues: list[str] = []
        recommendations: list[str] = []
        focus_areas: list[str] = []

        analysis: dict[str, Any] = {
            "priority_levels": {
                "critical": critical_issues,
                "high": high_issues,
                "medium": medium_issues,
                "low": low_issues,
            },
            "recommendations": recommendations,
            "estimated_effort": "unknown",
            "focus_areas": focus_areas,
        }

        # Simple analysis based on SonarQube data
        sources = data.get("sources", {})

        if "bugs_by_severity" in sources:
            bugs_data = sources["bugs_by_severity"]
            if isinstance(bugs_data, dict):
                critical_bugs = bugs_data.get("critical", 0)
                if str(critical_bugs).isdigit() and int(critical_bugs) > 5:
                    critical_issues.append(f"{critical_bugs} critical bugs found")
                    recommendations.append("Address critical bugs immediately")
                    focus_areas.append("Bug Resolution")

        if "code_smells_critical" in sources:
            smells_data = sources["code_smells_critical"]
            if isinstance(smells_data, dict):
                critical_smells = smells_data.get("critical_count", 0)
                if str(critical_smells).isdigit() and int(critical_smells) > 20:
                    high_issues.append(f"{critical_smells} critical code smells")
                    recommendations.append("Plan code smell remediation in next cycle")
                    focus_areas.append("Code Quality")

        if "coverage_analysis" in sources:
            coverage_data = sources["coverage_analysis"]
            if isinstance(coverage_data, dict):
                projects_low_coverage = coverage_data.get("projects_below_60", 0)
                if (
                    str(projects_low_coverage).isdigit()
                    and int(projects_low_coverage) > 0
                ):
                    medium_issues.append(
                        f"{projects_low_coverage} projects with low coverage"
                    )
                    recommendations.append(
                        "Improve test coverage for identified projects"
                    )
                    focus_areas.append("Test Coverage")

        return analysis
