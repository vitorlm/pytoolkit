"""
Quarterly Review Prompts for MCP Integration.

This module provides specialized prompts for quarterly and cycle-based analysis,
replacing traditional sprint-based prompts with quarterly/cycle structure.
"""

from typing import Any

from mcp.types import GetPromptResult, Prompt, PromptArgument

from ..adapters.jira_adapter import JiraAdapter
from ..adapters.linearb_adapter import LinearBAdapter

from .base_prompt import BasePromptHandler


class QuarterlyReviewPromptHandler(BasePromptHandler):
    """
    Handler for quarterly/cycle review prompts.

    Combines data from:
    - JIRA: Progress by quarter/cycle, velocity, cycle time
    - LinearB: Engineering metrics, PR performance by quarters
    """

    def __init__(self) -> None:
        super().__init__("QuarterlyReview")
        self.jira_adapter = JiraAdapter()
        self.linearb_adapter = LinearBAdapter()

    def get_prompt_definitions(self) -> list[Prompt]:
        """Define quarterly review prompts."""
        return [
            Prompt(
                name="quarterly_cycle_analysis",
                description="Complete quarterly/cycle analysis with integrated JIRA and LinearB data",
                arguments=[
                    PromptArgument(
                        name="project_key",
                        description="JIRA project key",
                        required=True,
                    ),
                    PromptArgument(
                        name="quarter_cycle",
                        description="Period in format Q1-C1, Q2-C2, etc. or 'current'",
                        required=False,
                    ),
                    PromptArgument(
                        name="include_recommendations",
                        description="Include recommendations for next cycle",
                        required=False,
                    ),
                ],
            ),
            Prompt(
                name="quarterly_retrospective_data",
                description="Structured data for quarterly/cycle retrospective",
                arguments=[
                    PromptArgument(
                        name="project_key",
                        description="JIRA project key",
                        required=True,
                    ),
                    PromptArgument(
                        name="quarter_cycle",
                        description="Period for retrospective",
                        required=False,
                    ),
                ],
            ),
            Prompt(
                name="cycle_planning_insights",
                description="Insights for next cycle planning based on historical data",
                arguments=[
                    PromptArgument(
                        name="project_key",
                        description="JIRA project key",
                        required=True,
                    ),
                    PromptArgument(
                        name="cycles_history",
                        description="Number of cycles for historical analysis (default: 6)",
                        required=False,
                    ),
                ],
            ),
        ]

    async def get_prompt_content(self, name: str, arguments: dict[str, Any]) -> GetPromptResult:
        """Generates specific prompt content."""
        if name == "quarterly_cycle_analysis":
            return await self._generate_quarterly_cycle_analysis(arguments)
        elif name == "quarterly_retrospective_data":
            return await self._generate_quarterly_retrospective_data(arguments)
        elif name == "cycle_planning_insights":
            return await self._generate_cycle_planning_insights(arguments)
        else:
            raise ValueError(f"Unknown quarterly prompt: {name}")

    async def _generate_quarterly_cycle_analysis(self, args: dict[str, Any]) -> GetPromptResult:
        """Generates complete quarterly/cycle analysis."""
        project_key = args["project_key"]
        quarter_cycle = args.get("quarter_cycle", "current")
        include_recommendations = args.get("include_recommendations", True)

        # Parse period
        period_info = self.parse_quarter_cycle(quarter_cycle)

        def _collect_quarterly_data(project_key: str, period_info: dict, include_recs: bool) -> dict[str, Any]:
            data = {
                "period_info": period_info,
                "timestamp": self.get_current_timestamp(),
            }

            try:
                # JIRA quarterly data
                data["quarterly_jira_data"] = {
                    "velocity": self.jira_adapter.get_velocity_analysis(project_key, time_period="last-quarter"),
                    "cycle_time": self.jira_adapter.get_cycle_time_analysis(project_key, time_period="last-quarter"),
                    "adherence": self.jira_adapter.get_adherence_analysis(project_key, time_period="last-quarter"),
                }

                # Cycle metrics
                data["cycle_metrics"] = self.jira_adapter.get_cycle_time_analysis(
                    project_key, time_period="last-3-months"
                )

                # LinearB quarterly metrics
                data["linearb_quarterly_metrics"] = {
                    "engineering_metrics": self.linearb_adapter.get_engineering_metrics("last-quarter"),
                    "team_performance": self.linearb_adapter.get_team_performance(),
                }

                if include_recs:
                    # Historical comparison
                    data["historical_comparison"] = {
                        "velocity_6m": self.jira_adapter.get_velocity_analysis(
                            project_key, time_period="last-6-months"
                        ),
                        "cycle_time_6m": self.jira_adapter.get_cycle_time_analysis(
                            project_key, time_period="last-6-months"
                        ),
                    }

            except Exception as e:
                data["error"] = str(e)
                data["note"] = "Some data sources may be unavailable"

            return data

        quarterly_data = self.cached_prompt_generation(
            "quarterly_cycle_analysis",
            _collect_quarterly_data,
            expiration_minutes=120,  # Longer cache for quarterly data
            project_key=project_key,
            period_info=period_info,
            include_recs=include_recommendations,
        )

        system_content = self.create_quarterly_context(period_info["quarter"], period_info["cycle"])

        system_content += """
**Task**: Perform comprehensive quarterly/cycle analysis.

**Analysis Areas**:
1. Cycle goal achievement and deliverables
2. Team velocity trends across cycles
3. Cycle time performance evolution
4. Issue adherence to cycle deadlines
5. Engineering productivity metrics by quarter
6. Bottlenecks and impediments identification
7. Cross-cycle team performance insights

**Output Format**:
- Executive Summary
- Key Metrics Analysis (Quarterly Context)
- Achievements & Challenges by Cycle
- Actionable Recommendations for Next Cycle
- Quarterly Planning Considerations
"""

        data_content = self.format_data_for_prompt(quarterly_data, f"Quarterly Data - {period_info['period_code']}")

        user_content = f"""Analyze this quarterly/cycle data and provide comprehensive review:

{data_content}

Focus on cycle-specific insights and recommendations for sustainable quarterly delivery."""

        return GetPromptResult(
            description=f"Quarterly cycle analysis for {period_info['period_code']}",
            messages=[
                self.create_system_message(system_content),
                self.create_user_message(user_content),
            ],
        )

    async def _generate_quarterly_retrospective_data(self, args: dict[str, Any]) -> GetPromptResult:
        """Generates data for quarterly retrospective."""
        project_key = args["project_key"]
        quarter_cycle = args.get("quarter_cycle", "current")

        period_info = self.parse_quarter_cycle(quarter_cycle)

        def _collect_retrospective_data(project_key: str, period_info: dict) -> dict[str, Any]:
            data = {
                "period_info": period_info,
                "timestamp": self.get_current_timestamp(),
            }

            try:
                # JIRA cycle metrics for retrospective
                data["cycle_metrics"] = {
                    "velocity": self.jira_adapter.get_velocity_analysis(project_key, time_period="last-quarter"),
                    "cycle_time": self.jira_adapter.get_cycle_time_analysis(project_key, time_period="last-quarter"),
                    "completion_rate": self.jira_adapter.get_adherence_analysis(
                        project_key, time_period="last-quarter"
                    ),
                }

                # Quarterly progress analysis (using available epic monitoring)
                data["quarterly_progress"] = {
                    "epic_monitoring": self.jira_adapter.get_epic_monitoring_data(project_key)
                }

                # LinearB team performance metrics
                data["team_performance"] = {
                    "engineering_metrics": self.linearb_adapter.get_engineering_metrics("last-quarter"),
                    "team_productivity": self.linearb_adapter.get_team_performance(),
                }

            except Exception as e:
                data["error"] = str(e)
                data["note"] = "Some data sources may be unavailable for retrospective"

            return data

        retro_data = self.cached_prompt_generation(
            "quarterly_retrospective_data",
            _collect_retrospective_data,
            expiration_minutes=90,
            project_key=project_key,
            period_info=period_info,
        )

        system_content = self.create_quarterly_context(period_info["quarter"], period_info["cycle"])

        system_content += """
**Task**: Structure data for quarterly/cycle retrospective discussion.

**Focus Areas**:
- What went well (cycle successes)
- What didn't go well (cycle challenges)
- What can be improved (next cycle action items)
- Patterns in quarterly team behavior
- Process improvements for quarterly planning

**Format**: Organize findings into retrospective categories with supporting quarterly data.
"""

        data_content = self.format_data_for_prompt(retro_data, f"Retrospective Data - {period_info['period_code']}")

        user_content = f"""Structure this data for a quarterly/cycle retrospective meeting:

{data_content}

Identify patterns and areas for improvement within the quarterly/cycle context."""

        return GetPromptResult(
            description=f"Quarterly retrospective for {period_info['period_code']}",
            messages=[
                self.create_system_message(system_content),
                self.create_user_message(user_content),
            ],
        )

    async def _generate_cycle_planning_insights(self, args: dict[str, Any]) -> GetPromptResult:
        """Generates insights for next cycle planning."""
        project_key = args["project_key"]
        cycles_history = args.get("cycles_history", 6)

        def _collect_planning_data(project_key: str, cycles_history: int) -> dict[str, Any]:
            data = {
                "cycles_history": cycles_history,
                "timestamp": self.get_current_timestamp(),
            }

            try:
                # Historical velocity data for planning
                time_period = f"last-{cycles_history * 3}-months"  # Assuming 3-month cycles
                data["velocity_history"] = {
                    "historical_velocity": self.jira_adapter.get_velocity_analysis(project_key, time_period=time_period)
                }

                # Cycle performance trends
                data["cycle_trends"] = {
                    "cycle_time_analysis": self.jira_adapter.get_cycle_time_analysis(
                        project_key, time_period=time_period
                    )
                }

                # Quarterly patterns analysis (using available comprehensive dashboard)
                data["quarterly_patterns"] = {
                    "comprehensive_dashboard": self.jira_adapter.get_comprehensive_dashboard(project_key),
                    "team_performance": self.linearb_adapter.get_team_performance(),
                }

            except Exception as e:
                data["error"] = str(e)
                data["note"] = "Some historical data may be unavailable for planning analysis"

            return data

        planning_data = self.cached_prompt_generation(
            "cycle_planning_insights",
            _collect_planning_data,
            expiration_minutes=180,  # Planning data can have long cache
            project_key=project_key,
            cycles_history=cycles_history,
        )

        current_period = self.parse_quarter_cycle("current")
        system_content = self.create_quarterly_context(current_period["quarter"], current_period["cycle"])

        system_content += """
**Task**: Provide data-driven insights for next cycle planning.

**Planning Areas**:
- Recommended cycle capacity based on historical performance
- Story point estimation guidance using quarterly context
- Risk identification for upcoming cycle
- Team capacity considerations within quarterly goals
- Historical performance patterns across cycles

**Output**: Actionable planning recommendations with quarterly data support.
"""

        data_content = self.format_data_for_prompt(planning_data, f"Planning Data ({cycles_history} cycles history)")

        user_content = f"""Provide cycle planning insights based on this historical quarterly data:

{data_content}

Focus on cycle capacity planning, risk mitigation, and realistic goal setting within quarterly objectives."""

        return GetPromptResult(
            description=f"Cycle planning insights using {cycles_history} cycles history",
            messages=[
                self.create_system_message(system_content),
                self.create_user_message(user_content),
            ],
        )
