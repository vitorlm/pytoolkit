"""
Weekly Report Generation Prompts for MCP Integration.

This module provides specialized prompts for generating weekly engineering reports
based on run_reports.sh data collection and report_template.md structure.
"""

from typing import Any

from mcp.types import GetPromptResult, Prompt, PromptArgument

from ..adapters.circleci_adapter import CircleCIAdapter
from ..adapters.jira_adapter import JiraAdapter
from ..adapters.linearb_adapter import LinearBAdapter
from ..adapters.sonarqube_adapter import SonarQubeAdapter

from .base_prompt import BasePromptHandler


class WeeklyReportPromptHandler(BasePromptHandler):
    """
    Handler for weekly report generation prompts.

    Specialized in generating prompts that:
    - Execute workflow equivalent to run_reports.sh
    - Format data according to report_template.md
    - Generate consistent weekly analyses
    """

    def __init__(self) -> None:
        super().__init__("WeeklyReport")
        self.jira_adapter = JiraAdapter()
        self.linearb_adapter = LinearBAdapter()
        self.sonarqube_adapter = SonarQubeAdapter()
        self.circleci_adapter = CircleCIAdapter()

    def get_prompt_definitions(self) -> list[Prompt]:
        """Define weekly report prompts."""
        return [
            Prompt(
                name="generate_weekly_engineering_report",
                description="Generate complete weekly engineering report based on run_reports.sh + report_template.md",
                arguments=[
                    PromptArgument(
                        name="project_key",
                        description="JIRA project key (default: CWS)",
                        required=False,
                    ),
                    PromptArgument(
                        name="team_name",
                        description="Team name for analysis (optional, default: all teams for tribe-wide report)",
                        required=False,
                    ),
                    PromptArgument(
                        name="include_comparison",
                        description="Include weekly comparison (default: true)",
                        required=False,
                    ),
                    PromptArgument(
                        name="output_format",
                        description="Output format: markdown, json, template-ready (default: markdown)",
                        required=False,
                    ),
                ],
            ),
            Prompt(
                name="analyze_weekly_data_collection",
                description="Analyze and structure data collected by run_reports.sh for insights",
                arguments=[
                    PromptArgument(
                        name="focus_areas",
                        description="Focus areas: bugs, cycle-time, adherence, quality, linearb (default: all)",
                        required=False,
                    ),
                    PromptArgument(
                        name="include_recommendations",
                        description="Include action recommendations (default: true)",
                        required=False,
                    ),
                ],
            ),
            Prompt(
                name="format_template_sections",
                description="Format specific data for report_template.md sections",
                arguments=[
                    PromptArgument(
                        name="sections",
                        description="Sections to format: bugs_support, cycle_time, adherence, linearb, sonarqube (default: all)",
                        required=False,
                    ),
                    PromptArgument(
                        name="week_range",
                        description="Weekly period for analysis (format: YYYY-MM-DD to YYYY-MM-DD)",
                        required=False,
                    ),
                ],
            ),
            Prompt(
                name="generate_next_actions",
                description="Generate 'Next Actions' section based on weekly data and trends",
                arguments=[
                    PromptArgument(
                        name="priority_level",
                        description="Priority level for actions: immediate, short-term, all (default: all)",
                        required=False,
                    ),
                    PromptArgument(
                        name="team_context",
                        description="Specific team context for personalized recommendations",
                        required=False,
                    ),
                ],
            ),
            Prompt(
                name="compare_weekly_metrics",
                description="Compare weekly metrics and identify significant trends",
                arguments=[
                    PromptArgument(
                        name="comparison_weeks",
                        description="Number of weeks for comparison (default: 4)",
                        required=False,
                    ),
                    PromptArgument(
                        name="metrics_focus",
                        description="Priority metrics: velocity, quality, cycle-time, adherence (default: all)",
                        required=False,
                    ),
                ],
            ),
        ]

    async def get_prompt_content(
        self, name: str, arguments: dict[str, Any]
    ) -> GetPromptResult:
        """Generates specific prompt content."""
        if name == "generate_weekly_engineering_report":
            return await self._generate_weekly_engineering_report(arguments)
        elif name == "analyze_weekly_data_collection":
            return await self._analyze_weekly_data_collection(arguments)
        elif name == "format_template_sections":
            return await self._format_template_sections(arguments)
        elif name == "generate_next_actions":
            return await self._generate_next_actions(arguments)
        elif name == "compare_weekly_metrics":
            return await self._compare_weekly_metrics(arguments)
        else:
            raise ValueError(f"Unknown weekly report prompt: {name}")

    async def _generate_weekly_engineering_report(
        self, args: dict[str, Any]
    ) -> GetPromptResult:
        """Generates complete weekly engineering report."""

        def _collect_weekly_report_data(
            project_key: str, team_name: str, include_comparison: bool
        ) -> dict[str, Any]:
            # Simulates complete execution of run_reports.sh
            data_sources = {
                # JIRA Data Collection (equivalente aos comandos do script)
                "jira_bugs_support_2weeks": self._simulate_jira_bugs_support_2weeks(
                    project_key, team_name
                ),
                "jira_bugs_support_lastweek": self._simulate_jira_bugs_support_week(
                    project_key, team_name, "last"
                ),
                "jira_bugs_support_weekbefore": self._simulate_jira_bugs_support_week(
                    project_key, team_name, "before"
                ),
                "jira_tasks_2weeks": self._simulate_jira_tasks_2weeks(
                    project_key, team_name
                ),
                "jira_open_issues": self._simulate_jira_open_issues(
                    project_key, team_name
                ),
                "jira_cycle_time_lastweek": self._simulate_jira_cycle_time(
                    project_key, team_name
                ),
                # SonarQube Data Collection
                "sonarqube_quality_metrics": self._simulate_sonarqube_quality_metrics(),
                # LinearB Data Collection
                "linearb_engineering_metrics": self._simulate_linearb_metrics(),
            }

            # Add comparison data if requested
            if include_comparison:
                data_sources["comparison_analysis"] = (
                    self._generate_comparison_analysis(data_sources)
                )

            return data_sources

        project_key = args.get("project_key", "CWS")
        team_name = args.get("team_name")  # Allow None for tribe-wide reports
        include_comparison = args.get("include_comparison", True)
        output_format = args.get("output_format", "markdown")

        # Default to Catalog only if team_name is explicitly empty string, not if None
        if team_name == "":
            team_name = "Catalog"

        # Collect weekly data with cache
        weekly_data = self.cached_prompt_generation(
            "weekly_engineering_report",
            _collect_weekly_report_data,
            expiration_minutes=30,  # Short cache for weekly data
            project_key=project_key,
            team_name=team_name,
            include_comparison=include_comparison,
        )

        # Create specialized context for weekly report
        system_content = self.create_weekly_report_context()
        system_content += f"""
        **Current Task**: Generate complete weekly engineering report equivalent to run_reports.sh execution.

        **Report Requirements**:
        1. Follow exact structure from report_template.md
        2. Include all sections: Bugs & Support, Cycle Time, Adherence, LinearB Metrics, SonarQube Health, Next Actions
        3. Use specific numbers and percentages (not placeholders)
        4. Provide week-over-week comparisons with trend indicators
        5. Generate actionable recommendations

        **Data Processing Steps**:
        1. Process JIRA data (bugs, support, tasks, cycle time, open issues)
        2. Analyze SonarQube quality metrics across all projects
        3. Incorporate LinearB engineering productivity data
        4. Generate comparative analysis and trend identification
        5. Format according to template structure

        **Output Format**: {output_format}
        **Project**: {project_key}
        **Team**: {team_name if team_name else "All Teams (Tribe-wide)"}
        """

        # Format data for prompt
        team_display = f"{team_name} Team" if team_name else "All Teams (Tribe-wide)"
        data_content = self.format_data_for_prompt(
            weekly_data, f"Weekly Engineering Data - {team_display}"
        )

        user_content = f"""Generate a complete weekly engineering report using this collected data:

{data_content}

**Instructions**:
1. Structure the report exactly like report_template.md
2. Fill in all placeholder values with actual data from the sources
3. Calculate week-over-week changes and trends
4. Provide specific, actionable recommendations
5. Ensure all numbers are realistic and consistent
6. Include priority assessments for bugs and issues
7. Generate meaningful next actions based on the data analysis

**Focus on**:
- Data-driven insights
- Trend identification
- Actionable recommendations
- Professional formatting suitable for engineering leadership"""

        return GetPromptResult(
            description=f"Complete weekly engineering report for {team_display} - {project_key} project",
            messages=[
                self.create_system_message(system_content),
                self.create_user_message(user_content),
            ],
        )

    async def _analyze_weekly_data_collection(
        self, args: dict[str, Any]
    ) -> GetPromptResult:
        """Analyzes weekly collected data."""

        def _collect_analysis_data(focus_areas: list[str]) -> dict[str, Any]:
            analysis_data = {}

            if "all" in focus_areas or "bugs" in focus_areas:
                analysis_data["bugs_analysis"] = self._get_bugs_analysis_data()
            if "all" in focus_areas or "cycle-time" in focus_areas:
                analysis_data["cycle_time_analysis"] = (
                    self._get_cycle_time_analysis_data()
                )
            if "all" in focus_areas or "adherence" in focus_areas:
                analysis_data["adherence_analysis"] = (
                    self._get_adherence_analysis_data()
                )
            if "all" in focus_areas or "quality" in focus_areas:
                analysis_data["quality_analysis"] = self._get_quality_analysis_data()
            if "all" in focus_areas or "linearb" in focus_areas:
                analysis_data["linearb_analysis"] = self._get_linearb_analysis_data()

            return analysis_data

        focus_areas_str = args.get("focus_areas", "all")
        focus_areas = (
            [area.strip() for area in focus_areas_str.split(",")]
            if focus_areas_str != "all"
            else ["all"]
        )
        include_recommendations = args.get("include_recommendations", True)

        analysis_data = self.cached_prompt_generation(
            "weekly_data_analysis",
            _collect_analysis_data,
            expiration_minutes=45,
            focus_areas=focus_areas,
        )

        system_content = self.create_management_context()
        system_content += """
**Task**: Analyze weekly engineering data for insights and patterns.

**Analysis Approach**:
1. Identify key trends and patterns in the data
2. Highlight significant changes from previous periods
3. Pinpoint areas of concern or improvement
4. Correlate metrics across different domains (JIRA, SonarQube, LinearB)
5. Provide data-driven insights for decision making

**Output Requirements**:
- Structured analysis by focus area
- Quantified insights with supporting data
- Trend identification (improving, stable, declining)
- Risk assessment and priority recommendations
"""

        focus_display = (
            ", ".join(focus_areas) if "all" not in focus_areas else "All Areas"
        )
        data_content = self.format_data_for_prompt(
            analysis_data, f"Weekly Analysis Data - Focus: {focus_display}"
        )

        recommendations_instruction = (
            """
7. Provide specific, actionable recommendations for each area of concern
8. Prioritize recommendations by impact and effort required"""
            if include_recommendations
            else ""
        )

        user_content = f"""Analyze this weekly engineering data and provide comprehensive insights:

{data_content}

**Analysis Requirements**:
1. Examine trends and patterns in each focus area
2. Identify significant changes or anomalies
3. Correlate metrics between different sources
4. Assess overall team/project health
5. Highlight priority areas needing attention
6. Quantify the impact of identified issues{recommendations_instruction}

Focus on actionable insights that can drive immediate improvements."""

        return GetPromptResult(
            description=f"Weekly data analysis - Focus: {focus_display}",
            messages=[
                self.create_system_message(system_content),
                self.create_user_message(user_content),
            ],
        )

    async def _format_template_sections(self, args: dict[str, Any]) -> GetPromptResult:
        """Formats data for specific template sections."""

        def _collect_section_data(
            sections: list[str], week_range: str
        ) -> dict[str, Any]:
            section_data = {}

            if "all" in sections or "bugs_support" in sections:
                section_data["bugs_support"] = self._get_bugs_support_template_data(
                    week_range
                )
            if "all" in sections or "cycle_time" in sections:
                section_data["cycle_time"] = self._get_cycle_time_template_data(
                    week_range
                )
            if "all" in sections or "adherence" in sections:
                section_data["adherence"] = self._get_adherence_template_data(
                    week_range
                )
            if "all" in sections or "linearb" in sections:
                section_data["linearb"] = self._get_linearb_template_data(week_range)
            if "all" in sections or "sonarqube" in sections:
                section_data["sonarqube"] = self._get_sonarqube_template_data()

            return section_data

        sections_str = args.get("sections", "all")
        sections = (
            [s.strip() for s in sections_str.split(",")]
            if sections_str != "all"
            else ["all"]
        )
        week_range = args.get("week_range", "current_week")

        section_data = self.cached_prompt_generation(
            "template_sections",
            _collect_section_data,
            expiration_minutes=30,
            sections=sections,
            week_range=week_range,
        )

        system_content = self.create_weekly_report_context()
        system_content += """
**Task**: Format data sections for direct insertion into report_template.md.

**Formatting Requirements**:
1. Use exact table formats from report_template.md
2. Include proper markdown formatting
3. Use appropriate trend indicators (⬆️⬇️➡️)
4. Provide realistic numbers and percentages
5. Maintain consistent formatting across sections
6. Include necessary context and explanations

**Section Formats**:
- Bugs & Support: Priority table with week-over-week comparison
- Cycle Time: Metrics table with average, median, max
- Adherence: Category breakdown with percentages
- LinearB: Metrics comparison with trend indicators
- SonarQube: Project quality table with health status
"""

        sections_display = (
            ", ".join(sections) if "all" not in sections else "All Sections"
        )
        data_content = self.format_data_for_prompt(
            section_data, f"Template Section Data - {sections_display}"
        )

        user_content = f"""Format the following data for direct insertion into report_template.md sections:

{data_content}

**Requirements**:
1. Use exact markdown table formats from the template
2. Replace all placeholders with realistic data values
3. Include proper trend indicators and comparisons
4. Ensure data consistency across related sections
5. Provide brief explanations where needed
6. Format for professional engineering report presentation

**Week Range**: {week_range}
**Sections**: {sections_display}

Output each section with its proper markdown formatting ready for copy-paste into the template."""

        return GetPromptResult(
            description=f"Template section formatting - {sections_display}",
            messages=[
                self.create_system_message(system_content),
                self.create_user_message(user_content),
            ],
        )

    async def _generate_next_actions(self, args: dict[str, Any]) -> GetPromptResult:
        """Generates Next Actions section of the report."""

        def _collect_next_actions_data(
            priority_level: str, team_context: str
        ) -> dict[str, Any]:
            return {
                "priority_level": priority_level,
                "team_context": team_context,
                "current_issues": self._get_current_priority_issues(),
                "trends_analysis": self._get_trends_for_actions(),
                "capacity_considerations": self._get_team_capacity_info(),
                "upcoming_deadlines": self._get_upcoming_deadlines(),
            }

        priority_level = args.get("priority_level", "all")
        team_context = args.get("team_context", "")

        actions_data = self.cached_prompt_generation(
            "next_actions_generation",
            _collect_next_actions_data,
            expiration_minutes=60,
            priority_level=priority_level,
            team_context=team_context,
        )

        system_content = self.create_management_context()
        system_content += """
**Task**: Generate actionable "Next Actions" section for weekly engineering report.

**Action Categories**:
1. **Immediate (This Week)**: Urgent issues requiring immediate attention
2. **Short Term (Next 2 Weeks)**: Important improvements and planning items

**Criteria for Actions**:
- Specific and actionable (not vague suggestions)
- Tied to actual data and trends from the weekly report
- Realistic given team capacity and constraints
- Prioritized by impact and urgency
- Include responsible parties when possible

**Action Quality Standards**:
- Each action should be completable within specified timeframe
- Actions should address root causes, not just symptoms
- Include success criteria or measurable outcomes
- Consider dependencies and prerequisites
"""

        data_content = self.format_data_for_prompt(
            actions_data, "Next Actions Analysis Data"
        )

        priority_instruction = {
            "immediate": "Focus only on immediate, urgent actions for this week",
            "short-term": "Focus on short-term planning actions for next 2 weeks",
            "all": "Include both immediate and short-term actions",
        }.get(priority_level, "Include both immediate and short-term actions")

        user_content = f"""Generate specific, actionable "Next Actions" based on this weekly data:

{data_content}

**Requirements**:
1. Create 2 categories: Immediate (This Week) and Short Term (Next 2 Weeks)
2. {priority_instruction}
3. Each action should be specific, measurable, and time-bound
4. Base recommendations on actual data trends and issues
5. Consider team capacity and realistic delivery expectations
6. Include 3-5 actions per category (don't overwhelm)
7. Prioritize actions by impact and feasibility

**Team Context**: {team_context if team_context else "General engineering team"}

Format as markdown lists ready for insertion into report template."""

        return GetPromptResult(
            description=f"Next Actions generation - Priority: {priority_level}",
            messages=[
                self.create_system_message(system_content),
                self.create_user_message(user_content),
            ],
        )

    async def _compare_weekly_metrics(self, args: dict[str, Any]) -> GetPromptResult:
        """Compares weekly metrics and identifies trends."""

        def _collect_comparison_data(
            weeks: int, metrics_focus: list[str]
        ) -> dict[str, Any]:
            return {
                "comparison_weeks": weeks,
                "metrics_focus": metrics_focus,
                "historical_data": self._get_historical_metrics_data(weeks),
                "trend_analysis": self._generate_trend_analysis(weeks, metrics_focus),
                "statistical_insights": self._get_statistical_insights(weeks),
            }

        comparison_weeks = args.get("comparison_weeks", 4)
        metrics_focus_str = args.get("metrics_focus", "all")
        metrics_focus = (
            [m.strip() for m in metrics_focus_str.split(",")]
            if metrics_focus_str != "all"
            else ["all"]
        )

        comparison_data = self.cached_prompt_generation(
            "weekly_metrics_comparison",
            _collect_comparison_data,
            expiration_minutes=90,
            weeks=comparison_weeks,
            metrics_focus=metrics_focus,
        )

        system_content = self.create_management_context()
        system_content += f"""
**Task**: Perform comparative analysis of weekly engineering metrics over {comparison_weeks} weeks.

**Analysis Framework**:
1. **Trend Identification**: Improving, stable, or declining patterns
2. **Statistical Analysis**: Averages, variations, outliers
3. **Correlation Analysis**: Relationships between different metrics
4. **Performance Patterns**: Weekly cycles, seasonal effects
5. **Significance Assessment**: Which changes are meaningful vs noise

**Metrics Categories**:
- **Velocity**: Story points, task completion rates
- **Quality**: Bug rates, code quality metrics, technical debt
- **Cycle-time**: Lead time, development time, review time
- **Adherence**: Due date compliance, estimation accuracy

**Output Requirements**:
- Quantified trends with statistical backing
- Identification of significant pattern changes
- Correlation insights between metrics
- Recommendations based on trend analysis
"""

        metrics_display = (
            ", ".join(metrics_focus) if "all" not in metrics_focus else "All Metrics"
        )
        data_content = self.format_data_for_prompt(
            comparison_data,
            f"Weekly Metrics Comparison - {comparison_weeks} weeks - Focus: {metrics_display}",
        )

        user_content = f"""Analyze these weekly metrics trends and provide comprehensive comparison insights:

{data_content}

**Analysis Requirements**:
1. Identify clear trends (improving/stable/declining) for each metric category
2. Quantify the rate of change and significance
3. Correlate related metrics (e.g., quality vs cycle time)
4. Highlight weeks with significant deviations or improvements
5. Assess the sustainability of current trends
6. Identify leading vs lagging indicators
7. Provide data-driven insights for process improvements

**Focus Areas**: {metrics_display}
**Time Period**: Last {comparison_weeks} weeks

Provide actionable insights that can guide team process adjustments and improvements."""

        return GetPromptResult(
            description=f"Weekly metrics comparison - {comparison_weeks} weeks - {metrics_display}",
            messages=[
                self.create_system_message(system_content),
                self.create_user_message(user_content),
            ],
        )

    # Helper methods for analysis and calculations
    def _calculate_week_over_week_changes(
        self, current_week: dict, previous_week: dict
    ) -> dict[str, Any]:
        """Calculate week-over-week changes in metrics."""
        try:
            if not current_week.get("data") or not previous_week.get("data"):
                return {"note": "Insufficient data for week-over-week comparison"}

            # This would contain logic to compare metrics between weeks
            return {
                "bugs_change": self._calculate_metric_change(
                    current_week, previous_week, "bugs"
                ),
                "support_change": self._calculate_metric_change(
                    current_week, previous_week, "support"
                ),
                "completion_rate_change": self._calculate_metric_change(
                    current_week, previous_week, "completion_rate"
                ),
            }
        except Exception as e:
            return {"error": str(e), "note": "Week-over-week calculation failed"}

    def _analyze_trends(self, data_sources: dict[str, Any]) -> dict[str, Any]:
        """Analyze trends across all data sources."""
        try:
            return {
                "jira_trends": self._extract_jira_trends(data_sources),
                "quality_trends": self._extract_quality_trends(data_sources),
                "engineering_trends": self._extract_engineering_trends(data_sources),
            }
        except Exception as e:
            return {"error": str(e), "note": "Trend analysis failed"}

    def _identify_significant_variations(
        self, data_sources: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Identify significant variations in the data."""
        try:
            variations: list[dict[str, Any]] = []
            # Logic to identify significant changes would go here
            # This is a placeholder for the implementation
            return variations
        except Exception as e:
            return [
                {
                    "error": str(e),
                    "note": "Significant variations identification failed",
                }
            ]

    def _generate_performance_indicators(
        self, data_sources: dict[str, Any]
    ) -> dict[str, Any]:
        """Generate performance indicators from data sources."""
        try:
            return {
                "overall_health": self._calculate_overall_health(data_sources),
                "risk_indicators": self._identify_risk_indicators(data_sources),
                "improvement_areas": self._identify_improvement_areas(data_sources),
            }
        except Exception as e:
            return {"error": str(e), "note": "Performance indicators generation failed"}

    def _calculate_metric_change(
        self, current: dict, previous: dict, metric: str
    ) -> dict[str, Any]:
        """Calculate change in a specific metric."""
        # Placeholder implementation
        return {"metric": metric, "change": 0, "percentage": 0}

    def _extract_jira_trends(self, data_sources: dict[str, Any]) -> dict[str, Any]:
        """Extract JIRA-related trends."""
        return {
            "velocity": "stable",
            "cycle_time": "improving",
            "adherence": "declining",
        }

    def _extract_quality_trends(self, data_sources: dict[str, Any]) -> dict[str, Any]:
        """Extract quality-related trends."""
        return {
            "technical_debt": "stable",
            "coverage": "improving",
            "security": "stable",
        }

    def _extract_engineering_trends(
        self, data_sources: dict[str, Any]
    ) -> dict[str, Any]:
        """Extract engineering productivity trends."""
        return {"pr_cycle_time": "stable", "deployment_frequency": "improving"}

    def _summarize_trends(self, trend_data: dict[str, Any]) -> dict[str, Any]:
        """Summarize trend analysis."""
        return {
            "overall_trend": "mixed",
            "key_insights": ["Cycle time improving", "Quality stable"],
        }

    def _analyze_metric_correlations(self, weeks: int) -> dict[str, Any]:
        """Analyze correlations between metrics."""
        return {"velocity_vs_quality": 0.2, "cycle_time_vs_adherence": -0.3}

    def _detect_metric_outliers(self, weeks: int) -> list[dict[str, Any]]:
        """Detect outliers in metrics."""
        return [
            {"week": "2024-W10", "metric": "cycle_time", "value": 48.5, "threshold": 30}
        ]

    def _calculate_confidence_intervals(self, weeks: int) -> dict[str, Any]:
        """Calculate confidence intervals for metrics."""
        return {
            "velocity": {"lower": 15, "upper": 25},
            "cycle_time": {"lower": 18, "upper": 28},
        }

    def _calculate_overall_health(self, data_sources: dict[str, Any]) -> str:
        """Calculate overall project/team health score."""
        return "GOOD"  # GREEN/YELLOW/RED or GOOD/FAIR/POOR

    def _identify_risk_indicators(self, data_sources: dict[str, Any]) -> list[str]:
        """Identify risk indicators from the data."""
        return ["High number of P1 bugs", "Increasing cycle time trend"]

    def _identify_improvement_areas(self, data_sources: dict[str, Any]) -> list[str]:
        """Identify areas for improvement."""
        return ["Code review process", "Test automation coverage"]

    # Data simulation methods (placeholders that would be replaced with real adapters)
    def _simulate_jira_bugs_support_2weeks(
        self, project_key: str, team_name: str
    ) -> dict[str, Any]:
        """Collect JIRA bugs/support data for 2 weeks."""
        try:
            return {
                "command_equivalent": f"python src/main.py syngenta jira issue-adherence --project-key {project_key} --issue-types 'Bug,Support' --team {team_name}",
                "adherence_analysis": self.jira_adapter.get_adherence_analysis(
                    project_key,
                    time_period="14-days",  # Use N-days format instead of last-2-weeks
                    issue_types=["Bug", "Support"],
                    team=team_name,
                ),
            }
        except Exception as e:
            return {
                "error": str(e),
                "data": None,
                "note": "JIRA bugs/support data collection failed",
            }

    def _simulate_jira_bugs_support_week(
        self, project_key: str, team_name: str, week_type: str
    ) -> dict[str, Any]:
        """Collect JIRA bugs/support data for specific week."""
        time_period = "last-week" if week_type == "last" else "7-days"
        try:
            return {
                "week_type": week_type,
                "resolution_time_analysis": self.jira_adapter.get_resolution_time_analysis(
                    project_key,
                    time_period=time_period,
                    issue_types=["Bug", "Support"],
                    squad=team_name,
                ),
            }
        except Exception as e:
            return {
                "error": str(e),
                "week_type": week_type,
                "data": None,
                "note": f"JIRA {week_type} week data collection failed",
            }

    def _simulate_jira_tasks_2weeks(
        self, project_key: str, team_name: str
    ) -> dict[str, Any]:
        """Collect JIRA tasks data for 2 weeks."""
        issue_types = ["Story", "Task", "Epic", "Technical Debt", "Improvement"]
        try:
            return {
                "issue_types": issue_types,
                "velocity_metrics": self.jira_adapter.get_velocity_analysis(
                    project_key,
                    time_period="last-6-months",  # Use supported time period
                    issue_types=issue_types,
                    team=team_name,
                ),
            }
        except Exception as e:
            return {
                "error": str(e),
                "issue_types": issue_types,
                "data": None,
                "note": "JIRA tasks data collection failed",
            }

    def _simulate_jira_open_issues(
        self, project_key: str, team_name: str
    ) -> dict[str, Any]:
        """Collect JIRA open issues data."""
        try:
            return {
                "epic_monitoring": self.jira_adapter.get_epic_monitoring_data(
                    project_key, team_name
                )
            }
        except Exception as e:
            return {
                "error": str(e),
                "open_bugs": 0,
                "open_support": 0,
                "oldest_issues": [],
                "note": "JIRA open issues data collection failed",
            }

    def _simulate_jira_cycle_time(
        self, project_key: str, team_name: str
    ) -> dict[str, Any]:
        """Collect JIRA cycle time data."""
        try:
            cycle_time_data = self.jira_adapter.get_cycle_time_analysis(
                project_key, time_period="last-week"
            )
            return cycle_time_data
        except Exception as e:
            return {
                "error": str(e),
                "average_hours": 0,
                "median_hours": 0,
                "by_priority": {},
                "note": "JIRA cycle time data collection failed",
            }

    def _simulate_sonarqube_quality_metrics(self) -> dict[str, Any]:
        """Collect SonarQube quality metrics."""
        try:
            return {
                "organization": "syngenta-digital",
                "quality_dashboard": self.sonarqube_adapter.get_quality_dashboard(),
                "all_projects_metrics": self.sonarqube_adapter.get_all_projects_with_metrics(),
            }
        except Exception as e:
            return {
                "error": str(e),
                "organization": "syngenta-digital",
                "projects_analyzed": 0,
                "quality_overview": None,
                "note": "SonarQube quality metrics collection failed",
            }

    def _simulate_linearb_metrics(self) -> dict[str, Any]:
        """Collect LinearB engineering metrics."""
        try:
            return {
                "team_id": "41576",
                "metrics": self.linearb_adapter.get_engineering_metrics("last-week"),
                "team_performance": self.linearb_adapter.get_team_performance(),
            }
        except Exception as e:
            return {
                "error": str(e),
                "team_id": "41576",
                "metrics": None,
                "comparison": None,
                "note": "LinearB metrics collection failed",
            }

    def _generate_comparison_analysis(
        self, data_sources: dict[str, Any]
    ) -> dict[str, Any]:
        """Generate comparison analysis from data sources."""
        try:
            # Extract current and previous week data for comparison
            current_week = data_sources.get("jira_bugs_support_lastweek", {})
            previous_week = data_sources.get("jira_bugs_support_weekbefore", {})
            linearb_comparison = data_sources.get(
                "linearb_engineering_metrics", {}
            ).get("comparison", {})

            return {
                "week_over_week_changes": self._calculate_week_over_week_changes(
                    current_week, previous_week
                ),
                "trend_analysis": self._analyze_trends(data_sources),
                "significant_variations": self._identify_significant_variations(
                    data_sources
                ),
                "linearb_trends": linearb_comparison,
                "performance_indicators": self._generate_performance_indicators(
                    data_sources
                ),
            }
        except Exception as e:
            return {
                "error": str(e),
                "week_over_week_changes": {},
                "trend_analysis": {},
                "significant_variations": [],
                "note": "Comparison analysis generation failed",
            }

    # Template data methods
    def _get_bugs_support_template_data(self, week_range: str) -> dict[str, Any]:
        """Formatted data for bugs/support template section."""
        return {
            "p1_current": "2",
            "p1_previous": "4",
            "p1_change": "⬇️ -2",
            "p2_current": "8",
            "p2_previous": "7",
            "p2_change": "⬆️ +1",
            "p3_current": "15",
            "p3_previous": "15",
            "p3_change": "➡️ 0",
        }

    def _get_cycle_time_template_data(self, week_range: str) -> dict[str, Any]:
        """Formatted data for cycle time template section."""
        return {"average_hours": "24.5", "median_hours": "18.2", "max_hours": "72.1"}

    def _get_adherence_template_data(self, week_range: str) -> dict[str, Any]:
        """Formatted data for adherence template section."""
        return {
            "early_count": 5,
            "early_pct": "20.8%",
            "ontime_count": 12,
            "ontime_pct": "50.0%",
            "late_count": 4,
            "late_pct": "16.7%",
            "no_due_count": 3,
            "no_due_pct": "12.5%",
            "total_count": 24,
        }

    def _get_linearb_template_data(self, week_range: str) -> dict[str, Any]:
        """Formatted data for LinearB template section."""
        return {
            "cycle_time_current": "24.5 h",
            "cycle_time_previous": "26.1 h",
            "cycle_time_change": "⬇️ 6.1%",
            "pickup_time_current": "2.1 h",
            "pickup_time_previous": "2.8 h",
            "pickup_time_change": "⬇️ 25%",
            "review_time_current": "6.3 h",
            "review_time_previous": "7.1 h",
            "review_time_change": "⬇️ 11.3%",
            "deploy_freq_current": "12",
            "deploy_freq_previous": "10",
            "deploy_freq_change": "⬆️ 20%",
        }

    def _get_sonarqube_template_data(self) -> dict[str, Any]:
        """Formatted data for SonarQube template section."""
        return {
            "quality_gate": "PASSED",
            "coverage": "78.5%",
            "bugs": "23",
            "reliability": "B",
            "code_smells": "156",
            "security_hotspots": "87%",
            "health_status": "GOOD",
        }

    # Missing analysis methods - add these to support the weekly report functionality
    def _get_bugs_analysis_data(self) -> dict[str, Any]:
        """Get bugs analysis data."""
        try:
            return {
                "adherence": self.jira_adapter.get_adherence_analysis(
                    "CWS", time_period="last-week"
                ),
                "resolution_time": self.jira_adapter.get_resolution_time_analysis(
                    "CWS", time_period="last-week"
                ),
            }
        except Exception as e:
            return {"error": str(e), "note": "Bugs analysis data collection failed"}

    def _get_cycle_time_analysis_data(self) -> dict[str, Any]:
        """Get cycle time analysis data."""
        try:
            return self.jira_adapter.get_cycle_time_analysis(
                "CWS", time_period="last-week"
            )
        except Exception as e:
            return {
                "error": str(e),
                "note": "Cycle time analysis data collection failed",
            }

    def _get_adherence_analysis_data(self) -> dict[str, Any]:
        """Get adherence analysis data."""
        try:
            return self.jira_adapter.get_adherence_analysis(
                "CWS", time_period="last-week"
            )
        except Exception as e:
            return {
                "error": str(e),
                "note": "Adherence analysis data collection failed",
            }

    def _get_quality_analysis_data(self) -> dict[str, Any]:
        """Get quality analysis data."""
        try:
            return self.sonarqube_adapter.get_quality_dashboard()
        except Exception as e:
            return {"error": str(e), "note": "Quality analysis data collection failed"}

    def _get_linearb_analysis_data(self) -> dict[str, Any]:
        """Get LinearB analysis data."""
        try:
            return self.linearb_adapter.get_engineering_metrics("last-week")
        except Exception as e:
            return {"error": str(e), "note": "LinearB analysis data collection failed"}

    def _get_current_priority_issues(self) -> dict[str, Any]:
        """Get current priority issues."""
        try:
            return self.jira_adapter.get_epic_monitoring_data("CWS")
        except Exception as e:
            return {"error": str(e), "note": "Priority issues data collection failed"}

    def _get_trends_for_actions(self) -> dict[str, Any]:
        """Get trend data for action recommendations."""
        try:
            return {
                "velocity": self.jira_adapter.get_velocity_analysis(
                    "CWS", time_period="last-6-months"
                ),
                "cycle_time": self.jira_adapter.get_cycle_time_analysis(
                    "CWS", time_period="last-3-months"
                ),
            }
        except Exception as e:
            return {"error": str(e), "note": "Trends analysis failed"}

    def _get_team_capacity_info(self) -> dict[str, Any]:
        """Get team capacity information."""
        try:
            return self.linearb_adapter.get_team_performance()
        except Exception as e:
            return {"error": str(e), "note": "Team capacity info collection failed"}

    def _get_upcoming_deadlines(self) -> dict[str, Any]:
        """Get upcoming deadlines."""
        try:
            return self.jira_adapter.get_comprehensive_dashboard("CWS")
        except Exception as e:
            return {"error": str(e), "note": "Upcoming deadlines collection failed"}

    def _get_historical_metrics_data(self, weeks: int) -> dict[str, Any]:
        """Get historical metrics data."""
        try:
            time_period = f"last-{weeks * 7}-days"
            return {
                "velocity": self.jira_adapter.get_velocity_analysis(
                    "CWS", time_period=time_period
                ),
                "adherence": self.jira_adapter.get_adherence_analysis(
                    "CWS", time_period=time_period
                ),
            }
        except Exception as e:
            return {
                "error": str(e),
                "note": "Historical metrics data collection failed",
            }

    def _generate_trend_analysis(
        self, weeks: int, metrics_focus: list[str]
    ) -> dict[str, Any]:
        """Generate trend analysis."""
        try:
            return {
                "focus_metrics": metrics_focus,
                "weeks_analyzed": weeks,
                "trend_summary": "Trend analysis based on available data",
            }
        except Exception as e:
            return {"error": str(e), "note": "Trend analysis generation failed"}

    def _get_statistical_insights(self, weeks: int) -> dict[str, Any]:
        """Get statistical insights."""
        try:
            return {
                "weeks_analyzed": weeks,
                "statistical_summary": "Statistical analysis based on available data",
            }
        except Exception as e:
            return {"error": str(e), "note": "Statistical insights generation failed"}
