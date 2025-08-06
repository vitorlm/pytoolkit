import json
from typing import Any

from mcp.types import TextContent, Tool

from src.mcp_server.adapters.jira_adapter import JiraAdapter
from utils.logging.logging_manager import LogManager


class JiraTools:
    """Tools MCP para integração com JIRA via PyToolkit."""

    def __init__(self):
        """
        Initialize JIRA Tools MCP handler.

        Sets up the JIRA adapter and logger for processing JIRA-related
        MCP tool requests.
        """
        self.adapter = JiraAdapter()
        self.logger = LogManager.get_instance().get_logger("JiraTools")

    @staticmethod
    def get_tool_definitions() -> list[Tool]:
        """Retorna definições de todas as JIRA tools."""
        return [
            Tool(
                name="jira_get_epic_monitoring",
                description=(
                    "Obtém dados de monitoramento de épicos JIRA com status, "
                    "datas e problemas identificados"
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "project_key": {
                            "type": "string",
                            "description": "Chave do projeto JIRA (ex: 'SCRUM', 'DEV')",
                        }
                    },
                    "required": ["project_key"],
                },
            ),
            Tool(
                name="jira_get_cycle_time_metrics",
                description="Obtém métricas de cycle time para análise de performance da equipe",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "project_key": {
                            "type": "string",
                            "description": "Chave do projeto JIRA",
                        },
                        "days_back": {
                            "type": "integer",
                            "description": "Número de dias para análise histórica (padrão: 30)",
                            "default": 30,
                        },
                    },
                    "required": ["project_key"],
                },
            ),
            Tool(
                name="jira_get_team_velocity",
                description="Obtém dados de velocidade da equipe baseado em sprints anteriores",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "project_key": {
                            "type": "string",
                            "description": "Chave do projeto JIRA",
                        },
                        "sprints": {
                            "type": "integer",
                            "description": "Número de sprints para análise (padrão: 5)",
                            "default": 5,
                        },
                    },
                    "required": ["project_key"],
                },
            ),
            Tool(
                name="jira_get_issue_adherence",
                description="Analisa aderência da equipe a prazos e datas de entrega",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "project_key": {
                            "type": "string",
                            "description": "Chave do projeto JIRA",
                        }
                    },
                    "required": ["project_key"],
                },
            ),
        ]

    async def execute_tool(
        self, name: str, arguments: dict[str, Any]
    ) -> list[TextContent]:
        """Executa tool JIRA específica."""
        self.logger.info(f"Executing JIRA tool: {name} with args: {arguments}")

        try:
            if name == "jira_get_epic_monitoring":
                return await self._get_epic_monitoring(arguments)
            elif name == "jira_get_cycle_time_metrics":
                return await self._get_cycle_time_metrics(arguments)
            elif name == "jira_get_team_velocity":
                return await self._get_team_velocity(arguments)
            elif name == "jira_get_issue_adherence":
                return await self._get_issue_adherence(arguments)
            else:
                error_msg = f"Unknown JIRA tool '{name}'"
                self.logger.error(error_msg)
                return [TextContent(type="text", text=f"Error: {error_msg}")]
        except Exception as e:
            error_msg = f"Error executing JIRA tool '{name}': {str(e)}"
            self.logger.error(error_msg)
            return [TextContent(type="text", text=error_msg)]

    async def _get_epic_monitoring(self, args: dict[str, Any]) -> list[TextContent]:
        """
        Execute epic monitoring analysis.

        Retrieves and formats epic monitoring data including status, dates,
        and identified problems for the specified project.

        Args:
            args: Dictionary containing project_key and other parameters

        Returns:
            list[TextContent]: Formatted epic monitoring results

        Raises:
            Exception: If epic monitoring data retrieval fails
        """
        project_key = args["project_key"]
        self.logger.info(f"Getting epic monitoring data for project: {project_key}")

        try:
            data = self.adapter.get_epic_monitoring_data(project_key)

            formatted_result = {
                "project_key": project_key,
                "epic_monitoring": data,
                "summary": {
                    "total_epics": (
                        len(data.get("epics", [])) if isinstance(data, dict) else "N/A"
                    ),
                    "timestamp": (
                        data.get("timestamp") if isinstance(data, dict) else None
                    ),
                },
            }

            return [
                TextContent(
                    type="text",
                    text=f"Epic Monitoring Data for {project_key}:\n{json.dumps(formatted_result, indent=2)}",
                )
            ]
        except Exception as e:
            self.logger.error(f"Failed to get epic monitoring data: {e}")
            return [
                TextContent(
                    type="text",
                    text=f"Failed to retrieve epic monitoring data for {project_key}: {str(e)}",
                )
            ]

    async def _get_cycle_time_metrics(self, args: dict[str, Any]) -> list[TextContent]:
        """
        Execute cycle time analysis.

        Analyzes cycle time metrics for issues in the specified project,
        providing insights into team performance and delivery efficiency.

        Args:
            args: Dictionary containing project_key and optional days_back parameter

        Returns:
            list[TextContent]: Formatted cycle time analysis results

        Raises:
            Exception: If cycle time analysis fails
        """
        project_key = args["project_key"]
        days_back = args.get("days_back", 30)
        self.logger.info(
            f"Getting cycle time metrics for {project_key}, last {days_back} days"
        )

        try:
            data = self.adapter.get_cycle_time_analysis(project_key, days_back)

            formatted_result = {
                "project_key": project_key,
                "analysis_period_days": days_back,
                "cycle_time_data": data,
                "summary": {
                    "total_issues_analyzed": (
                        len(data.get("issues", [])) if isinstance(data, dict) else "N/A"
                    ),
                    "timestamp": (
                        data.get("timestamp") if isinstance(data, dict) else None
                    ),
                },
            }

            return [
                TextContent(
                    type="text",
                    text=f"Cycle Time Metrics for {project_key} (last {days_back} days):\n"
                    f"{json.dumps(formatted_result, indent=2)}",
                )
            ]
        except Exception as e:
            self.logger.error(f"Failed to get cycle time metrics: {e}")
            return [
                TextContent(
                    type="text",
                    text=f"Failed to retrieve cycle time metrics for {project_key}: {str(e)}",
                )
            ]

    async def _get_team_velocity(self, args: dict[str, Any]) -> list[TextContent]:
        """
        Execute team velocity analysis.

        Analyzes team velocity based on historical sprint data, providing
        insights into team productivity and capacity planning.

        Args:
            args: Dictionary containing project_key and optional sprints parameter

        Returns:
            list[TextContent]: Formatted team velocity analysis results

        Raises:
            Exception: If velocity analysis fails
        """
        project_key = args["project_key"]
        sprints = args.get("sprints", 5)
        self.logger.info(
            f"Getting team velocity for {project_key}, last {sprints} sprints"
        )

        try:
            data = self.adapter.get_velocity_analysis(project_key, sprints)

            formatted_result = {
                "project_key": project_key,
                "sprints_analyzed": sprints,
                "velocity_data": data,
                "summary": {
                    "analysis_type": "team_velocity",
                    "timestamp": (
                        data.get("timestamp") if isinstance(data, dict) else None
                    ),
                },
            }

            return [
                TextContent(
                    type="text",
                    text=f"Team Velocity for {project_key} (last {sprints} sprints):\n{json.dumps(formatted_result, indent=2)}",
                )
            ]
        except Exception as e:
            self.logger.error(f"Failed to get team velocity: {e}")
            return [
                TextContent(
                    type="text",
                    text=f"Failed to retrieve team velocity for {project_key}: {str(e)}",
                )
            ]

    async def _get_issue_adherence(self, args: dict[str, Any]) -> list[TextContent]:
        """
        Execute issue adherence analysis.

        Analyzes how well the team adheres to due dates and delivery commitments,
        providing insights into planning accuracy and delivery reliability.

        Args:
            args: Dictionary containing project_key

        Returns:
            list[TextContent]: Formatted adherence analysis results

        Raises:
            Exception: If adherence analysis fails
        """
        project_key = args["project_key"]
        self.logger.info(f"Getting issue adherence analysis for {project_key}")

        try:
            data = self.adapter.get_adherence_analysis(project_key)

            formatted_result = {
                "project_key": project_key,
                "adherence_analysis": data,
                "summary": {
                    "analysis_type": "issue_adherence",
                    "timestamp": (
                        data.get("timestamp") if isinstance(data, dict) else None
                    ),
                },
            }

            return [
                TextContent(
                    type="text",
                    text=f"Issue Adherence Analysis for {project_key}:\n{json.dumps(formatted_result, indent=2)}",
                )
            ]
        except Exception as e:
            self.logger.error(f"Failed to get issue adherence: {e}")
            return [
                TextContent(
                    type="text",
                    text=f"Failed to retrieve issue adherence analysis for {project_key}: {str(e)}",
                )
            ]
