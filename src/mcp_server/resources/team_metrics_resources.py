"""
Team Metrics Resources for MCP Integration.

This module provides team metrics resources that aggregate data from JIRA and LinearB
for comprehensive team performance analysis using quarterly/cycle structure.
"""

from typing import Any

from mcp.types import Resource, TextResourceContents
from pydantic import AnyUrl

from ..adapters.jira_adapter import JiraAdapter
from ..adapters.linearb_adapter import LinearBAdapter

from .base_resource import BaseResourceHandler


class TeamMetricsResourceHandler(BaseResourceHandler):
    """
    Handler for team metrics resources.

    Aggregates data from:
    - JIRA: Velocity, cycle time, adherence, bugs/support
    - LinearB: Engineering metrics, performance
    - Support for quarter/cycle structure (Q1 C1, Q1 C2, etc.)
    """

    def __init__(self):
        """
        Initialize Team Metrics Resource Handler.

        Sets up adapters for JIRA and LinearB integration to provide
        comprehensive team performance analysis resources.
        """
        super().__init__("TeamMetrics")
        self.jira_adapter = JiraAdapter()
        self.linearb_adapter = LinearBAdapter()

    def get_resource_definitions(self) -> list[Resource]:
        """
        Define available team metrics resources.

        Returns:
            list[Resource]: list of available team metrics resources including
                          performance dashboard, quarterly summary, health indicators,
                          and weekly metrics.
        """
        return [
            Resource(
                uri=AnyUrl("team://performance_dashboard"),
                name="Team Performance Dashboard",
                description="Consolidated team performance dashboard with JIRA + LinearB data by quarter/cycle",
                mimeType="text/markdown",
            ),
            Resource(
                uri=AnyUrl("team://quarterly_summary"),
                name="Quarterly Summary Report",
                description="Consolidated quarterly report with integrated metrics (replaces sprint summary)",
                mimeType="text/markdown",
            ),
            Resource(
                uri=AnyUrl("team://health_indicators"),
                name="Team Health Indicators",
                description="Team health indicators based on multiple metrics",
                mimeType="text/markdown",
            ),
            Resource(
                uri=AnyUrl("team://weekly_metrics"),
                name="Weekly Team Metrics",
                description="Weekly team metrics based on run_reports.sh for regular reporting",
                mimeType="text/markdown",
            ),
        ]

    async def get_resource_content(self, uri: str) -> TextResourceContents:
        """
        Retrieve resource content based on URI.

        Args:
            uri: Resource URI identifying which team metrics resource to generate

        Returns:
            TextResourceContents: Formatted resource content in markdown format

        Raises:
            ValueError: If URI is not recognized
        """
        if uri == "team://performance_dashboard":
            return await self._get_performance_dashboard()
        elif uri == "team://quarterly_summary":
            return await self._get_quarterly_summary()
        elif uri == "team://health_indicators":
            return await self._get_health_indicators()
        elif uri == "team://weekly_metrics":
            return await self._get_weekly_metrics()
        else:
            raise ValueError(f"Unknown team resource URI: {uri}")

    async def _get_performance_dashboard(self) -> TextResourceContents:
        """
        Generate team performance dashboard.

        Creates a comprehensive dashboard aggregating data from JIRA and LinearB
        including velocity, cycle time, engineering metrics, and adherence analysis.

        Returns:
            TextResourceContents: Formatted dashboard content with integrated metrics
        """

        def _generate_dashboard() -> dict[str, Any]:
            # Define fontes de dados para performance dashboard
            data_sources = {
                "current_cycle_velocity": lambda: self._get_current_cycle_data(),
                "jira_cycle_time": lambda: self.jira_adapter.get_cycle_time_analysis("CWS", time_period="last-month"),
                "linearb_engineering": lambda: self.linearb_adapter.get_engineering_metrics("last-month"),
                "linearb_team_performance": lambda: self.linearb_adapter.get_team_performance(),
                "recent_adherence": lambda: self.jira_adapter.get_adherence_analysis("CWS"),
            }

            # JIRA é obrigatório, LinearB é opcional
            return self.aggregate_data_safely(data_sources, required_sources=["current_cycle_velocity"])

        # Cache por 2 horas (dashboard é pesado)
        dashboard_data = self.cached_resource_operation(
            "performance_dashboard", _generate_dashboard, expiration_minutes=120
        )

        # Adiciona informações de período
        period_info = self.parse_quarter_cycle("current")
        dashboard_data["period_info"] = period_info

        content = self.format_resource_content(
            dashboard_data,
            "Team Performance Dashboard",
            f"Consolidated view of team performance metrics from JIRA and LinearB\n{self.format_quarter_cycle_summary(period_info)}",
        )

        return TextResourceContents(
            uri=AnyUrl("team://performance_dashboard"),
            mimeType="text/markdown",
            text=content,
        )

    async def _get_quarterly_summary(self) -> TextResourceContents:
        """
        Generate quarterly summary report.

        Creates a consolidated quarterly analysis integrating JIRA epic monitoring,
        adherence analysis, velocity metrics, and LinearB engineering data.

        Returns:
            TextResourceContents: Formatted quarterly summary with period context
        """

        def _generate_quarterly_summary() -> dict[str, Any]:
            # Determina período atual
            period_info = self.parse_quarter_cycle("current")
            days_in_cycle = self.get_period_days(period_info["quarter"], period_info["cycle"])

            data_sources = {
                "quarterly_jira_data": lambda: self.jira_adapter.get_epic_monitoring_data("CWS"),
                "cycle_adherence": lambda: self.jira_adapter.get_adherence_analysis("CWS"),
                "cycle_velocity": lambda: self.jira_adapter.get_velocity_analysis("CWS"),  # Current cycle
                "linearb_cycle_metrics": lambda: self.linearb_adapter.get_engineering_metrics(
                    "last-2-months"
                ),  # Broader for quarterly view
                "bugs_support_summary": lambda: self._get_bugs_support_summary(days_in_cycle),
            }

            aggregated = self.aggregate_data_safely(data_sources, required_sources=["quarterly_jira_data"])
            aggregated["period_info"] = period_info

            return aggregated

        quarterly_data = self.cached_resource_operation(
            "quarterly_summary",
            _generate_quarterly_summary,
            expiration_minutes=180,  # Cache por 3 horas para dados quarterly
        )

        period_info = quarterly_data.get("period_info", self.parse_quarter_cycle("current"))

        content = self.format_resource_content(
            quarterly_data,
            f"Quarterly Summary Report - {period_info['period_code']}",
            f"Comprehensive quarterly analysis with JIRA and LinearB data integration\n{self.format_quarter_cycle_summary(period_info)}",
        )

        return TextResourceContents(
            uri=AnyUrl("team://quarterly_summary"),
            mimeType="text/markdown",
            text=content,
        )

    async def _get_health_indicators(self) -> TextResourceContents:
        """Gera indicadores de saúde da equipe."""

        def _generate_health_indicators() -> dict[str, Any]:
            data_sources = {
                "velocity_trend": lambda: self.jira_adapter.get_velocity_analysis("CWS"),  # Current analysis
                "cycle_time_trend": lambda: self.jira_adapter.get_cycle_time_analysis(
                    "CWS", time_period="last-2-months"
                ),
                "adherence_rate": lambda: self.jira_adapter.get_adherence_analysis("CWS"),
                "engineering_health": lambda: self.linearb_adapter.get_team_performance(),
                "bugs_health": lambda: self._get_bugs_health_metrics(),
            }

            raw_data = self.aggregate_data_safely(data_sources)

            # Processa dados para gerar indicadores de saúde
            health_analysis = self._analyze_team_health(raw_data)
            raw_data["health_analysis"] = health_analysis
            raw_data["period_info"] = self.parse_quarter_cycle("current")

            return raw_data

        health_data = self.cached_resource_operation(
            "health_indicators",
            _generate_health_indicators,
            expiration_minutes=90,  # Cache por 1.5 horas
        )

        period_info = health_data.get("period_info", self.parse_quarter_cycle("current"))

        content = self.format_resource_content(
            health_data,
            f"Team Health Indicators - {period_info['period_code']}",
            f"AI-powered analysis of team health based on multiple data sources\n{self.format_quarter_cycle_summary(period_info)}",
        )

        return TextResourceContents(
            uri=AnyUrl("team://health_indicators"),
            mimeType="text/markdown",
            text=content,
        )

    async def _get_weekly_metrics(self) -> TextResourceContents:
        """Gera métricas semanais baseadas no run_reports.sh."""

        def _generate_weekly_metrics() -> dict[str, Any]:
            # Simula dados que seriam gerados pelo run_reports.sh
            data_sources = {
                "jira_bugs_support_weekly": lambda: self._get_weekly_bugs_support(),
                "jira_cycle_time_weekly": lambda: self.jira_adapter.get_cycle_time_analysis(
                    "CWS", time_period="last-week"
                ),  # Last week
                "jira_adherence_weekly": lambda: self.jira_adapter.get_adherence_analysis("CWS"),
                "linearb_weekly": lambda: self.linearb_adapter.get_engineering_metrics("last-week"),
                "sonarqube_current": lambda: self._get_sonarqube_weekly_summary(),
                "open_issues_summary": lambda: self._get_open_issues_summary(),
            }

            weekly_data = self.aggregate_data_safely(data_sources, required_sources=["jira_bugs_support_weekly"])

            # Adiciona metadados do relatório semanal
            weekly_data["report_metadata"] = {
                "template_compatible": True,
                "based_on_run_reports_sh": True,
                "report_sections": [
                    "Bugs & Support Overview",
                    "Cycle Time Summary",
                    "Adherence Analysis",
                    "LinearB Metrics Comparison",
                    "SonarCloud Quality & Security Health",
                ],
            }

            return weekly_data

        weekly_data = self.cached_resource_operation(
            "weekly_metrics",
            _generate_weekly_metrics,
            expiration_minutes=30,  # Cache curto para dados semanais
        )

        content = self.format_resource_content(
            weekly_data,
            "Weekly Team Metrics",
            "Weekly engineering metrics compatible with report_template.md format, based on run_reports.sh data collection",
        )

        return TextResourceContents(uri=AnyUrl("team://weekly_metrics"), mimeType="text/markdown", text=content)

    def _get_current_cycle_data(self) -> dict[str, Any]:
        """Obtém dados do ciclo atual."""
        period_info = self.parse_quarter_cycle("current")

        # Aproximação: usa velocity de 1 período para representar ciclo atual
        velocity_data = self.jira_adapter.get_velocity_analysis("CWS")

        return {
            "period": period_info["period_code"],
            "quarter": period_info["quarter"],
            "cycle": period_info["cycle"],
            "velocity_data": velocity_data,
            "days_in_cycle": self.get_period_days(period_info["quarter"], period_info["cycle"]),
        }

    def _get_bugs_support_summary(self, days_back: int) -> dict[str, Any]:
        """Obtém resumo de bugs e support para o período."""
        # Simulação baseada no que o run_reports.sh coletaria
        return {
            "bugs_resolved": "placeholder_data",
            "support_resolved": "placeholder_data",
            "adherence_rate": "placeholder_data",
            "period_days": days_back,
            "note": "Data would come from JIRA issue-adherence command with Bug,Support types",
        }

    def _get_bugs_health_metrics(self) -> dict[str, Any]:
        """Obtém métricas de saúde relacionadas a bugs."""
        return {
            "open_critical_bugs": "placeholder_count",
            "open_high_bugs": "placeholder_count",
            "avg_resolution_time": "placeholder_hours",
            "note": "Data would come from JIRA open-issues command",
        }

    def _get_weekly_bugs_support(self) -> dict[str, Any]:
        """Obtém dados semanais de bugs e support baseados no run_reports.sh."""
        return {
            "last_week": {
                "bugs_resolved": "placeholder_data",
                "support_resolved": "placeholder_data",
                "adherence_percentage": "placeholder_percentage",
            },
            "week_before": {
                "bugs_resolved": "placeholder_data",
                "support_resolved": "placeholder_data",
                "adherence_percentage": "placeholder_percentage",
            },
            "comparison": "placeholder_comparison_data",
            "data_source": "Equivalent to jira issue-adherence commands in run_reports.sh",
        }

    def _get_sonarqube_weekly_summary(self) -> dict[str, Any]:
        """Obtém resumo semanal do SonarQube."""
        return {
            "quality_gate_status": "placeholder_status",
            "critical_issues": "placeholder_count",
            "coverage_percentage": "placeholder_percentage",
            "note": "Data would come from SonarQube list-projects command",
        }

    def _get_open_issues_summary(self) -> dict[str, Any]:
        """Obtém resumo de issues em aberto."""
        return {
            "open_bugs": "placeholder_count",
            "open_support": "placeholder_count",
            "oldest_issue_days": "placeholder_days",
            "data_source": "Equivalent to jira open-issues command in run_reports.sh",
        }

    def _analyze_team_health(self, data: dict[str, Any]) -> dict[str, Any]:
        """Analisa dados para gerar indicadores de saúde."""
        # Create explicit dictionaries and lists for type clarity
        indicators: dict[str, str] = {}
        recommendations: list[str] = []

        analysis: dict[str, Any] = {
            "overall_health": "good",  # good, warning, critical
            "indicators": indicators,
            "recommendations": recommendations,
            "quarter_cycle_context": True,
        }

        # Análise simples baseada em dados disponíveis
        sources = data.get("sources", {})

        # Velocity analysis
        if "velocity_trend" in sources:
            indicators["velocity"] = "stable"

        # Cycle time analysis
        if "cycle_time_trend" in sources:
            indicators["cycle_time"] = "good"

        # Adherence analysis
        if "adherence_rate" in sources:
            indicators["adherence"] = "good"

        # Bugs health
        if "bugs_health" in sources:
            indicators["bug_resolution"] = "monitoring"

        # Add recommendations based on analysis
        if data.get("errors"):
            recommendations.append("Some data sources are unavailable - consider investigating connectivity issues")

        recommendations.append("Continue monitoring quarterly/cycle metrics for trends")

        return analysis
