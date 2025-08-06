"""
Team Performance Prompts for MCP Integration.

This module provides specialized prompts for team performance analysis and improvement planning.
"""

from typing import Any

from mcp.types import GetPromptResult, Prompt, PromptArgument

from src.mcp_server.adapters.jira_adapter import JiraAdapter
from src.mcp_server.adapters.linearb_adapter import LinearBAdapter

from .base_prompt import BasePromptHandler


class TeamPerformancePromptHandler(BasePromptHandler):
    """
    Handler para prompts de performance de equipe.

    Combina dados de:
    - JIRA: Team velocity, cycle time, adherence
    - LinearB: Engineering productivity, PR metrics
    """

    def __init__(self) -> None:
        super().__init__("TeamPerformance")
        self.jira_adapter = JiraAdapter()
        self.linearb_adapter = LinearBAdapter()

    def get_prompt_definitions(self) -> list[Prompt]:
        """Define prompts de performance de equipe."""
        return [
            Prompt(
                name="team_health_assessment",
                description="Complete team health assessment with multiple metrics",
                arguments=[
                    PromptArgument(
                        name="project_key",
                        description="JIRA project key",
                        required=True,
                    ),
                    PromptArgument(
                        name="time_period",
                        description="Analysis period (last-month, last-quarter)",
                        required=False,
                    ),
                ],
            ),
            Prompt(
                name="productivity_improvement_plan",
                description="Data-based productivity improvement plan",
                arguments=[
                    PromptArgument(name="project_key", description="Project key", required=True),
                    PromptArgument(
                        name="focus_areas",
                        description="Specific focus areas (velocity, cycle-time, quality)",
                        required=False,
                    ),
                ],
            ),
        ]

    async def get_prompt_content(self, name: str, arguments: dict[str, Any]) -> GetPromptResult:
        """Gera conteúdo do prompt específico."""
        if name == "team_health_assessment":
            return await self._generate_team_health_assessment(arguments)
        elif name == "productivity_improvement_plan":
            return await self._generate_productivity_improvement_plan(arguments)
        else:
            raise ValueError(f"Unknown team performance prompt: {name}")

    async def _generate_team_health_assessment(self, args: dict[str, Any]) -> GetPromptResult:
        """Gera avaliação de saúde da equipe."""
        project_key = args["project_key"]
        time_period = args.get("time_period", "last-month")

        def _collect_team_health_data(project_key: str, time_period: str) -> dict[str, Any]:
            # Collect real data from adapters
            health_data: dict[str, Any] = {
                "timestamp": self.get_current_timestamp(),
                "project_key": project_key,
                "time_period": time_period,
            }

            try:
                # JIRA data
                health_data["velocity"] = self.jira_adapter.get_velocity_analysis(
                    project_key, time_period
                )
                health_data["cycle_time"] = self.jira_adapter.get_cycle_time_analysis(
                    project_key, time_period
                )
                health_data["adherence"] = self.jira_adapter.get_adherence_analysis(
                    project_key, time_period
                )

                # LinearB data
                health_data["engineering_metrics"] = self.linearb_adapter.get_engineering_metrics(
                    time_period
                )
                health_data["team_performance"] = self.linearb_adapter.get_team_performance()

            except Exception as e:
                health_data["error"] = str(e)
                health_data["note"] = "Some data sources may be unavailable"

            return health_data

        health_data = self.cached_prompt_generation(
            "team_health_assessment",
            _collect_team_health_data,
            expiration_minutes=90,
            project_key=project_key,
            time_period=time_period,
        )

        system_content = self.create_management_context()
        system_content += """
**Task**: Assess overall team health and performance.

**Health Indicators**:
1. Velocity stability and trends
2. Cycle time performance
3. Deadline adherence
4. Code quality trends
5. Team collaboration metrics
6. Productivity indicators
7. Workload distribution
"""

        data_content = self.format_data_for_prompt(health_data, f"Team Health Data - {project_key}")

        user_content = f"""Assess the health of this development team:

{data_content}

Provide comprehensive health assessment with specific areas for improvement."""

        return GetPromptResult(
            description=f"Team health assessment for {project_key}",
            messages=[
                self.create_system_message(system_content),
                self.create_user_message(user_content),
            ],
        )

    async def _generate_productivity_improvement_plan(
        self, args: dict[str, Any]
    ) -> GetPromptResult:
        """Gera plano de melhoria de produtividade."""
        project_key = args["project_key"]
        focus_areas = args.get("focus_areas", "velocity,cycle-time,quality").split(",")

        def _collect_productivity_data(project_key: str, focus_areas: list[str]) -> dict[str, Any]:
            # Collect real productivity data from adapters
            productivity_data: dict[str, Any] = {
                "timestamp": self.get_current_timestamp(),
                "project_key": project_key,
                "focus_areas": focus_areas,
            }

            try:
                # JIRA productivity data
                if "velocity" in focus_areas:
                    productivity_data["velocity_analysis"] = (
                        self.jira_adapter.get_velocity_analysis(project_key, "last-3-months")
                    )

                if "cycle-time" in focus_areas:
                    productivity_data["cycle_time_analysis"] = (
                        self.jira_adapter.get_cycle_time_analysis(project_key, "last-month")
                    )

                if "quality" in focus_areas:
                    productivity_data["adherence_analysis"] = (
                        self.jira_adapter.get_adherence_analysis(project_key, "last-month")
                    )

                # LinearB productivity metrics
                productivity_data["engineering_metrics"] = (
                    self.linearb_adapter.get_engineering_metrics("last-month")
                )
                productivity_data["team_performance"] = self.linearb_adapter.get_team_performance()

            except Exception as e:
                productivity_data["error"] = str(e)
                productivity_data["note"] = "Some data sources may be unavailable"

            return productivity_data

        productivity_data = self.cached_prompt_generation(
            "productivity_improvement_plan",
            _collect_productivity_data,
            expiration_minutes=120,
            project_key=project_key,
            focus_areas=focus_areas,
        )

        system_content = self.create_management_context()
        system_content += """
**Task**: Create actionable productivity improvement plan.

**Plan Components**:
1. Current state analysis
2. Identified bottlenecks
3. Specific improvement initiatives
4. Success metrics and KPIs
5. Timeline and milestones
6. Resource requirements
7. Risk mitigation
"""

        data_content = self.format_data_for_prompt(
            productivity_data, f"Productivity Data - {project_key}"
        )

        user_content = f"""Create productivity improvement plan based on this data:

{data_content}

Focus on {", ".join(focus_areas)} and provide specific, actionable recommendations."""

        return GetPromptResult(
            description=f"Productivity improvement plan for {project_key}",
            messages=[
                self.create_system_message(system_content),
                self.create_user_message(user_content),
            ],
        )
