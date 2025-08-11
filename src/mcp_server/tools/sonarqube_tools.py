import json
from typing import Any

from mcp.types import TextContent, Tool

from ..adapters.sonarqube_adapter import SonarQubeAdapter
from utils.logging.logging_manager import LogManager


class SonarQubeTools:
    """Tools MCP para integração com SonarQube via PyToolkit."""

    def __init__(self):
        """
        Initialize SonarQube Tools MCP handler.

        Sets up the SonarQube adapter and logger for processing SonarQube-related
        MCP tool requests including quality metrics, issues, and project analysis.
        """
        self.adapter = SonarQubeAdapter()
        self.logger = LogManager.get_instance().get_logger("SonarQubeTools")

    @staticmethod
    def get_tool_definitions() -> list[Tool]:
        """Retorna definições de todas as SonarQube tools."""
        return [
            Tool(
                name="sonar_get_project_metrics",
                description="Obtém métricas de qualidade de código de um projeto específico",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "project_key": {
                            "type": "string",
                            "description": "Chave do projeto no SonarQube",
                        }
                    },
                    "required": ["project_key"],
                },
            ),
            Tool(
                name="sonar_get_project_issues",
                description="Obtém issues (bugs, vulnerabilidades, code smells) de um projeto",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "project_key": {
                            "type": "string",
                            "description": "Chave do projeto no SonarQube",
                        },
                        "issue_type": {
                            "type": "string",
                            "description": "Tipo de issue: BUG, VULNERABILITY, CODE_SMELL",
                            "enum": ["BUG", "VULNERABILITY", "CODE_SMELL"],
                            "default": "BUG",
                        },
                    },
                    "required": ["project_key"],
                },
            ),
            Tool(
                name="sonar_get_quality_overview",
                description="Obtém overview de qualidade de todos os projetos configurados",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "use_project_list": {
                            "type": "boolean",
                            "description": "Usar lista pré-definida de projetos Syngenta",
                            "default": True,
                        }
                    },
                    "required": [],
                },
            ),
            Tool(
                name="sonar_compare_projects_quality",
                description="Compara métricas de qualidade entre múltiplos projetos",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "project_keys": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Lista de chaves de projetos para comparar",
                        }
                    },
                    "required": ["project_keys"],
                },
            ),
        ]

    async def execute_tool(self, name: str, arguments: dict[str, Any]) -> list[TextContent]:
        """Executa tool SonarQube específica."""
        self.logger.info(f"Executing SonarQube tool: {name} with args: {arguments}")

        try:
            if name == "sonar_get_project_metrics":
                return await self._get_project_metrics(arguments)
            elif name == "sonar_get_project_issues":
                return await self._get_project_issues(arguments)
            elif name == "sonar_get_quality_overview":
                return await self._get_quality_overview(arguments)
            elif name == "sonar_compare_projects_quality":
                return await self._compare_projects_quality(arguments)
            else:
                error_msg = f"Unknown SonarQube tool '{name}'"
                self.logger.error(error_msg)
                return [TextContent(type="text", text=f"Error: {error_msg}")]
        except Exception as e:
            error_msg = f"Error executing SonarQube tool '{name}': {str(e)}"
            self.logger.error(error_msg)
            return [TextContent(type="text", text=error_msg)]

    async def _get_project_metrics(self, args: dict[str, Any]) -> list[TextContent]:
        """
        Retrieve project quality metrics.

        Gets comprehensive quality metrics for a specific SonarQube project
        including bugs, vulnerabilities, code smells, coverage, and ratings.

        Args:
            args: Dictionary containing project_key

        Returns:
            list[TextContent]: Formatted project metrics results

        Raises:
            Exception: If project metrics retrieval fails
        """
        project_key = args["project_key"]
        self.logger.info(f"Getting project metrics for: {project_key}")

        try:
            data = self.adapter.get_project_details(project_key)

            formatted_result = {
                "project_key": project_key,
                "metrics": data,
                "summary": {
                    "analysis_type": "project_metrics",
                    "metrics_count": (len(data.get("measures", [])) if isinstance(data, dict) else "N/A"),
                    "timestamp": (data.get("timestamp") if isinstance(data, dict) else None),
                },
            }

            return [
                TextContent(
                    type="text",
                    text=f"Quality Metrics for {project_key}:\n{json.dumps(formatted_result, indent=2)}",
                )
            ]
        except Exception as e:
            self.logger.error(f"Failed to get project metrics: {e}")
            return [
                TextContent(
                    type="text",
                    text=f"Failed to retrieve project metrics for {project_key}: {str(e)}",
                )
            ]

    async def _get_project_issues(self, args: dict[str, Any]) -> list[TextContent]:
        """
        Retrieve project issues.

        Gets issues (bugs, vulnerabilities, code smells) for a specific
        SonarQube project, optionally filtered by issue type.

        Args:
            args: Dictionary containing project_key and optional issue_type

        Returns:
            list[TextContent]: Formatted project issues results

        Raises:
            Exception: If project issues retrieval fails
        """
        project_key = args["project_key"]
        issue_type = args.get("issue_type", "BUG")
        self.logger.info(f"Getting {issue_type} issues for project: {project_key}")

        try:
            # Note: O método get_project_details pode incluir informações sobre issues
            # Vamos usar isso como base e filtrar por tipo se necessário
            data = self.adapter.get_project_details(project_key, include_issues=True)

            # Filtrar issues por tipo se disponível
            issues_data = data.get("issues", {})
            filtered_issues = issues_data.get(issue_type.lower(), issues_data)

            formatted_result = {
                "project_key": project_key,
                "issue_type": issue_type,
                "issues": filtered_issues,
                "summary": {
                    "total_issues": (len(filtered_issues) if isinstance(filtered_issues, list) else "N/A"),
                    "timestamp": (data.get("timestamp") if isinstance(data, dict) else None),
                },
            }

            return [
                TextContent(
                    type="text",
                    text=f"{issue_type} Issues for {project_key}:\n{json.dumps(formatted_result, indent=2)}",
                )
            ]
        except Exception as e:
            self.logger.error(f"Failed to get project issues: {e}")
            return [
                TextContent(
                    type="text",
                    text=f"Failed to retrieve {issue_type} issues for {project_key}: {str(e)}",
                )
            ]

    async def _get_quality_overview(self, args: dict[str, Any]) -> list[TextContent]:
        """
        Retrieve quality overview.

        Gets a comprehensive quality overview across all configured projects
        or the predefined Syngenta Digital project list.

        Args:
            args: Dictionary containing optional use_project_list parameter

        Returns:
            list[TextContent]: Formatted quality overview results

        Raises:
            Exception: If quality overview retrieval fails
        """
        use_project_list = args.get("use_project_list", True)
        self.logger.info(f"Getting quality overview, use_project_list: {use_project_list}")

        try:
            if use_project_list:
                data = self.adapter.get_all_projects_with_metrics()
            else:
                # Fallback para dashboard geral
                data = self.adapter.get_quality_dashboard()

            formatted_result = {
                "quality_overview": data,
                "configuration": {
                    "use_project_list": use_project_list,
                    "projects_analyzed": (len(data.get("projects", [])) if isinstance(data, dict) else "N/A"),
                },
                "summary": {
                    "analysis_type": "quality_overview",
                    "timestamp": (data.get("timestamp") if isinstance(data, dict) else None),
                },
            }

            return [
                TextContent(
                    type="text",
                    text=f"Quality Overview:\n{json.dumps(formatted_result, indent=2)}",
                )
            ]
        except Exception as e:
            self.logger.error(f"Failed to get quality overview: {e}")
            return [TextContent(type="text", text=f"Failed to retrieve quality overview: {str(e)}")]

    async def _compare_projects_quality(self, args: dict[str, Any]) -> list[TextContent]:
        """
        Compare quality metrics between projects.

        Analyzes and compares quality metrics across multiple projects,
        providing side-by-side comparison of key quality indicators.

        Args:
            args: Dictionary containing project_keys list

        Returns:
            list[TextContent]: Formatted quality comparison results

        Raises:
            Exception: If quality comparison fails
        """
        project_keys = args["project_keys"]
        self.logger.info(f"Comparing quality for projects: {project_keys}")

        try:
            comparison_data = {}

            for project_key in project_keys:
                self.logger.info(f"Getting metrics for project: {project_key}")
                project_data = self.adapter.get_project_details(project_key)
                comparison_data[project_key] = project_data

            # Criar sumário comparativo
            comparison_summary = {
                "projects_compared": len(project_keys),
                "projects": project_keys,
                "comparison_metrics": [
                    "quality_gate",
                    "bugs",
                    "vulnerabilities",
                    "code_smells",
                    "coverage",
                ],
                "timestamp": (comparison_data.get(project_keys[0], {}).get("timestamp") if project_keys else None),
            }

            formatted_result = {
                "comparison_summary": comparison_summary,
                "project_data": comparison_data,
                "analysis_type": "quality_comparison",
            }

            return [
                TextContent(
                    type="text",
                    text=f"Quality Comparison:\n{json.dumps(formatted_result, indent=2)}",
                )
            ]
        except Exception as e:
            self.logger.error(f"Failed to compare projects quality: {e}")
            return [TextContent(type="text", text=f"Failed to compare projects quality: {str(e)}")]
