"""
Quality Report Prompts for MCP Integration.

This module provides specialized prompts for code quality reporting and analysis.
"""

from typing import Any

from mcp.types import GetPromptResult, Prompt, PromptArgument

from src.mcp_server.adapters.circleci_adapter import CircleCIAdapter
from src.mcp_server.adapters.sonarqube_adapter import SonarQubeAdapter

from .base_prompt import BasePromptHandler


class QualityReportPromptHandler(BasePromptHandler):
    """
    Handler para prompts de relatórios de qualidade.

    Combina dados de:
    - SonarQube: Code quality, issues, security
    - CircleCI: Build success, pipeline health
    """

    def __init__(self) -> None:
        super().__init__("QualityReport")
        self.sonarqube_adapter = SonarQubeAdapter()
        self.circleci_adapter = CircleCIAdapter()

    def get_prompt_definitions(self) -> list[Prompt]:
        """Define prompts de qualidade."""
        return [
            Prompt(
                name="code_quality_report",
                description="Complete code quality report with SonarQube and CI/CD",
                arguments=[
                    PromptArgument(
                        name="project_key",
                        description="SonarQube project key",
                        required=False,
                    ),
                    PromptArgument(
                        name="include_trends",
                        description="Include trends analysis",
                        required=False,
                    ),
                ],
            ),
            Prompt(
                name="technical_debt_prioritization",
                description="Data-based technical debt prioritization",
                arguments=[
                    PromptArgument(name="project_key", description="Project key", required=True)
                ],
            ),
            Prompt(
                name="security_assessment",
                description="Security assessment with recommendations",
                arguments=[
                    PromptArgument(
                        name="include_all_projects",
                        description="Include all Syngenta projects",
                        required=False,
                    )
                ],
            ),
        ]

    async def get_prompt_content(self, name: str, arguments: dict[str, Any]) -> GetPromptResult:
        """Gera conteúdo do prompt específico."""
        if name == "code_quality_report":
            return await self._generate_code_quality_report(arguments)
        elif name == "technical_debt_prioritization":
            return await self._generate_technical_debt_prioritization(arguments)
        elif name == "security_assessment":
            return await self._generate_security_assessment(arguments)
        else:
            raise ValueError(f"Unknown quality prompt: {name}")

    async def _generate_code_quality_report(self, args: dict[str, Any]) -> GetPromptResult:
        """Gera relatório de qualidade de código."""
        project_key = args.get("project_key")
        include_trends = args.get("include_trends", False)

        def _collect_quality_data(project_key: str, include_trends: bool) -> dict[str, Any]:
            data: dict[str, Any] = {
                "timestamp": self.get_current_timestamp(),
                "project_key": project_key,
                "include_trends": include_trends,
            }

            try:
                # SonarQube quality data
                if project_key:
                    data["sonarqube_data"] = {
                        "project_details": self.sonarqube_adapter.get_project_details(
                            project_key, include_issues=True
                        )
                    }
                else:
                    data["sonarqube_data"] = {
                        "all_projects_overview": self.sonarqube_adapter.get_all_projects_with_metrics(),
                        "quality_dashboard": self.sonarqube_adapter.get_quality_dashboard(),
                    }

                # CI/CD pipeline health
                data["pipeline_health"] = {
                    "pipeline_status": self.circleci_adapter.get_pipeline_status(project_key),
                    "build_metrics": self.circleci_adapter.get_build_metrics(project_key),
                    "deployment_frequency": self.circleci_adapter.analyze_deployment_frequency(
                        project_key
                    ),
                }

                # Trends analysis if requested (using available historical data)
                if include_trends:
                    data["trends"] = {
                        "quality_dashboard": self.sonarqube_adapter.get_quality_dashboard(),
                        "build_metrics_extended": self.circleci_adapter.get_build_metrics(
                            project_key, limit=200
                        ),
                    }

            except Exception as e:
                data["error"] = str(e)
                data["note"] = "Some quality data sources may be unavailable"

            return data

        quality_data = self.cached_prompt_generation(
            "code_quality_report",
            _collect_quality_data,
            expiration_minutes=90,
            project_key=project_key,
            include_trends=include_trends,
        )

        system_content = self.create_management_context()
        system_content += """
**Task**: Generate comprehensive code quality report.

**Analysis Areas**:
1. Code quality metrics (maintainability, reliability, security)
2. Issue analysis and trends
3. Quality gate status
4. Technical debt assessment
5. Build/deployment success rates
6. Recommendations for improvement
"""

        target = project_key if project_key else "All Projects"
        data_content = self.format_data_for_prompt(quality_data, f"Code Quality Data - {target}")

        user_content = f"""Generate comprehensive code quality report based on this data:

{data_content}

Provide actionable insights and prioritized recommendations."""

        return GetPromptResult(
            description=f"Code quality report for {target}",
            messages=[
                self.create_system_message(system_content),
                self.create_user_message(user_content),
            ],
        )

    async def _generate_technical_debt_prioritization(
        self, args: dict[str, Any]
    ) -> GetPromptResult:
        """Gera priorização de débito técnico."""
        project_key = args["project_key"]

        def _collect_debt_data(project_key: str) -> dict[str, Any]:
            data: dict[str, Any] = {
                "timestamp": self.get_current_timestamp(),
                "project_key": project_key,
            }

            try:
                # Technical debt analysis from SonarQube
                data["technical_debt_data"] = {
                    "project_details": self.sonarqube_adapter.get_project_details(
                        project_key, include_issues=True
                    ),
                    "quality_dashboard": self.sonarqube_adapter.get_quality_dashboard(
                        focus_projects=[project_key]
                    ),
                }

                # Security-related technical debt (included in project details)
                data["security_debt"] = {
                    "security_analysis": "Security metrics included in project_details above"
                }

                # Reliability issues (included in project details)
                data["reliability_debt"] = {
                    "reliability_analysis": "Reliability metrics included in project_details above"
                }

            except Exception as e:
                data["error"] = str(e)
                data["note"] = "Some technical debt data may be unavailable"

            return data

        debt_data = self.cached_prompt_generation(
            "technical_debt_prioritization",
            _collect_debt_data,
            expiration_minutes=120,
            project_key=project_key,
        )

        system_content = self.create_management_context()
        system_content += """
**Task**: Prioritize technical debt items for maximum impact.

**Prioritization Criteria**:
1. Security impact (highest priority)
2. Maintainability impact
3. Development velocity impact
4. Business risk
5. Effort to fix
"""

        data_content = self.format_data_for_prompt(
            debt_data, f"Technical Debt Data - {project_key}"
        )

        user_content = f"""Prioritize technical debt items in this data:

{data_content}

Provide ranked action plan with effort estimates and business impact assessment."""

        return GetPromptResult(
            description=f"Technical debt prioritization for {project_key}",
            messages=[
                self.create_system_message(system_content),
                self.create_user_message(user_content),
            ],
        )

    async def _generate_security_assessment(self, args: dict[str, Any]) -> GetPromptResult:
        """Gera avaliação de segurança."""
        include_all_projects = args.get("include_all_projects", True)

        def _collect_security_data(include_all: bool) -> dict[str, Any]:
            data: dict[str, Any] = {
                "timestamp": self.get_current_timestamp(),
                "include_all_projects": include_all,
            }

            try:
                if include_all:
                    # Organization-wide security assessment
                    data["security_data"] = {
                        "all_projects_with_metrics": self.sonarqube_adapter.get_all_projects_with_metrics(),
                        "quality_dashboard": self.sonarqube_adapter.get_quality_dashboard(),
                    }
                else:
                    # Single project security assessment would need project_key parameter
                    data["security_data"] = {
                        "quality_dashboard": self.sonarqube_adapter.get_quality_dashboard()
                    }

                # Common security analysis (using available methods)
                data["security_analysis"] = {
                    "available_metrics": self.sonarqube_adapter.get_available_metrics()
                }

            except Exception as e:
                data["error"] = str(e)
                data["note"] = "Some security data sources may be unavailable"

            return data

        security_data = self.cached_prompt_generation(
            "security_assessment",
            _collect_security_data,
            expiration_minutes=60,
            include_all=include_all_projects,
        )

        system_content = self.create_management_context()
        system_content += """
**Task**: Conduct comprehensive security assessment.

**Assessment Areas**:
1. Security vulnerabilities (critical, high, medium, low)
2. Security rating trends
3. Security hotspots requiring review
4. Compliance with security standards
5. Risk assessment and mitigation strategies
"""

        scope = "All Projects" if include_all_projects else "Main Project"
        data_content = self.format_data_for_prompt(security_data, f"Security Data - {scope}")

        user_content = f"""Conduct security assessment based on this data:

{data_content}

Focus on risk prioritization and actionable security improvements."""

        return GetPromptResult(
            description=f"Security assessment for {scope}",
            messages=[
                self.create_system_message(system_content),
                self.create_user_message(user_content),
            ],
        )
