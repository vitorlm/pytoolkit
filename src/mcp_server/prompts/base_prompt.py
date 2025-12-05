"""Base Prompt Handler for MCP Integration with PyToolkit.

This module provides the base prompt handler class that all prompt handlers must inherit from.
It provides common functionality for generating parametrizable prompts with data integration.
"""

import json
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from mcp.types import GetPromptResult, Prompt, PromptMessage, TextContent

from utils.cache_manager.cache_manager import CacheManager
from utils.logging.logging_manager import LogManager


class BasePromptHandler(ABC):
    """Base handler for specialized MCP prompts.

    Provides:
    - Parametrizable templates
    - Integration with adapters and resources
    - Cache of generated prompts
    - Standardized formatting
    - Support for quartiles/cycles structure
    """

    def __init__(self, prompt_category: str) -> None:
        """Initialize base prompt handler.

        Args:
            prompt_category: Category name for logging and caching
        """
        self.prompt_category = prompt_category
        self.logger = LogManager.get_instance().get_logger("MCPPrompt", prompt_category)
        self.cache = CacheManager.get_instance()

        self.logger.info(f"Initializing {prompt_category} prompt handler")

    def get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        return datetime.now().isoformat()

    @abstractmethod
    def get_prompt_definitions(self) -> list[Prompt]:
        """Returns prompt definitions."""

    @abstractmethod
    async def get_prompt_content(self, name: str, arguments: dict[str, Any]) -> GetPromptResult:
        """Generates prompt content with parameters."""

    def get_cache_key(self, prompt_name: str, **kwargs) -> str:
        """Generates cache key for prompts."""
        params = "_".join([f"{k}_{v}" for k, v in sorted(kwargs.items())])
        return f"prompt_{self.prompt_category}_{prompt_name}_{params}"

    def cached_prompt_generation(self, prompt_name: str, func, expiration_minutes: int = 30, **kwargs) -> Any:
        """Generates prompt with cache (prompts are less durable than resources).

        Args:
            prompt_name: Prompt name
            func: Function to generate prompt
            expiration_minutes: Cache time (default: 30 minutes)
            **kwargs: Generation parameters
        """
        cache_key = self.get_cache_key(prompt_name, **kwargs)

        # Try to load from cache
        cached_result = self.cache.load(cache_key, expiration_minutes=expiration_minutes)
        if cached_result is not None:
            self.logger.debug(f"Prompt cache hit for {prompt_name}")
            return cached_result

        try:
            # Generate prompt
            self.logger.info(f"Generating prompt {prompt_name} - cache miss")
            result = func(**kwargs)

            # Save to cache
            self.cache.save(cache_key, result)
            self.logger.info(f"Cached prompt result for {prompt_name}")

            return result

        except Exception as e:
            self.logger.error(f"Error generating prompt {prompt_name}: {e}")
            raise

    def create_system_message(self, content: str) -> PromptMessage:
        """Creates standardized system message."""
        return PromptMessage(role="user", content=TextContent(type="text", text=f"System: {content}"))

    def create_user_message(self, content: str) -> PromptMessage:
        """Creates standardized user message."""
        return PromptMessage(role="user", content=TextContent(type="text", text=content))

    def format_data_for_prompt(self, data: dict[str, Any], title: str) -> str:
        """Formats data for inclusion in prompts."""
        formatted = f"## {title}\n\n"

        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, dict | list):
                    formatted += f"**{key}**: {json.dumps(value, indent=2)}\n\n"
                else:
                    formatted += f"**{key}**: {value}\n\n"
        else:
            formatted += f"{data}\n\n"

        return formatted

    def create_management_context(self, timestamp: str | None = None) -> str:
        """Creates standard context for management prompts."""
        timestamp = timestamp or datetime.now().isoformat()

        return f"""You are an AI assistant specialized in software development team management and project analysis.

        **Context**: You have access to integrated data from multiple sources:
        - JIRA: Issue tracking, team velocity, cycle time, quarterly/cycle metrics
        - SonarQube: Code quality metrics, technical debt, security issues
        - CircleCI: Build/deployment pipelines, success rates, performance
        - LinearB: Engineering productivity metrics, team performance

        **Work Structure**: Our team follows a quarterly/cycle structure:
        - Work is organized in 4 quarters (Q1, Q2, Q3, Q4)
        - Each quarter has 2 cycles (C1, C2)
        - Current periods are referenced as Q1-C1, Q1-C2, Q2-C1, Q2-C2, etc.
        - This replaces traditional sprint-based planning

        **Weekly Reports**: We generate weekly engineering reports based on:
        - run_reports.sh script that collects data from all sources
        - report_template.md structure for consistent reporting
        - Analysis covers: Bugs & Support, Cycle Time, Adherence, LinearB metrics, SonarQube quality

        **Generated**: {timestamp}

        **Guidelines**:
        - Provide actionable insights based on the data
        - Identify trends and patterns across metrics
        - Highlight areas needing attention
        - Suggest specific improvements
        - Use data to support recommendations
        - Focus on team productivity and code quality
        - Consider quarterly/cycle context in analysis
        - Format outputs for engineering teams and management

        """

    def create_weekly_report_context(self, timestamp: str | None = None) -> str:
        """Creates specific context for weekly reports."""
        timestamp = timestamp or datetime.now().isoformat()

        return f"""You are an AI assistant specialized in generating weekly engineering reports for software development teams.

        **Report Structure**: Based on report_template.md format with sections:
        1. Bugs & Support Overview (P1/P2/P3 priorities, week-over-week comparison)
        2. Cycle Time Summary (average, median, max by priority)
        3. Adherence Analysis (early, on-time, late, no due date)
        4. LinearB Metrics Comparison (cycle time, pickup time, review time, deploy frequency)
        5. SonarCloud Quality & Security Health (quality gates, coverage, issues)
        6. Next Actions (immediate and short-term)

        **Data Sources**:
        - JIRA (CWS project): Bug tracking, task completion, cycle time analysis
        - LinearB (Team ID 41576): Engineering productivity metrics
        - SonarCloud (syngenta-digital org): Code quality across 13+ projects

        **Generated**: {timestamp}

        **Output Requirements**:
        - Use exact format from report_template.md
        - Provide specific numbers and percentages
        - Include week-over-week comparisons with trend indicators (⬆️⬇️➡️)
        - Highlight priority issues and actionable recommendations
        - Maintain professional tone suitable for engineering leadership

        """

    def create_quarterly_context(self, quarter: int, cycle: int, timestamp: str | None = None) -> str:
        """Creates specific context for quarterly/cycle analysis."""
        timestamp = timestamp or datetime.now().isoformat()

        return f"""You are an AI assistant specialized in quarterly and cycle-based project analysis.

**Current Period**: Q{quarter}-C{cycle} (Quarter {quarter}, Cycle {cycle})

**Quarterly Structure**:
- Each quarter (~90 days) divided into 2 cycles (~45 days each)
- Cycle planning replaces traditional sprint planning
- Metrics tracked per cycle with quarterly rollups
- Focus on sustainable delivery pace over quarters

**Analysis Context**:
- Compare current cycle performance to previous cycles
- Track quarterly trends and patterns
- Identify cycle-specific bottlenecks and improvements
- Plan upcoming cycles based on historical data

**Generated**: {timestamp}

**Guidelines**:
- Frame all analysis in quarterly/cycle context
- Compare to previous Q{quarter}-C{1 if cycle == 2 else 2} and Q{quarter - 1 if quarter > 1 else 4} cycles
- Provide cycle-specific recommendations
- Consider quarterly objectives and deliverables

"""

    def parse_quarter_cycle(self, period: str) -> dict[str, Any]:
        """Analyzes period in Q1-C1, Q2-C2, etc. format.

        Args:
            period: Period in "Q1-C1" format or "current" for current period

        Returns:
            dict with quarter and cycle information
        """
        if period.lower() == "current":
            # Determina quartil/ciclo atual baseado na data
            current_date = datetime.now()
            quarter = ((current_date.month - 1) // 3) + 1

            # Simplification: considers first half of quartile as C1, second as C2
            days_in_quarter = current_date.day + ((current_date.month - 1) % 3) * 30
            cycle = 1 if days_in_quarter <= 45 else 2

            return {
                "quarter": quarter,
                "cycle": cycle,
                "period_code": f"Q{quarter}-C{cycle}",
                "is_current": True,
            }

        # Parse formato Q1-C1, Q2-C2, etc.
        try:
            parts = period.upper().split("-")
            quarter_part = parts[0].replace("Q", "")
            cycle_part = parts[1].replace("C", "")

            return {
                "quarter": int(quarter_part),
                "cycle": int(cycle_part),
                "period_code": period.upper(),
                "is_current": False,
            }
        except (IndexError, ValueError):
            self.logger.warning(f"Invalid period format '{period}', using current period")
            return self.parse_quarter_cycle("current")

    def format_template_section(self, section_name: str, data: dict[str, Any]) -> str:
        """Formats data for specific section of report_template.md."""
        formatted = f"### {section_name}\n\n"

        if section_name == "Bugs & Support Overview":
            formatted += self._format_bugs_support_section(data)
        elif section_name == "Cycle Time Summary":
            formatted += self._format_cycle_time_section(data)
        elif section_name == "Adherence Analysis":
            formatted += self._format_adherence_section(data)
        elif section_name == "LinearB Metrics":
            formatted += self._format_linearb_section(data)
        elif section_name == "SonarQube Health":
            formatted += self._format_sonarqube_section(data)
        else:
            # Generic formatting
            formatted += json.dumps(data, indent=2)

        return formatted

    def _format_bugs_support_section(self, data: dict[str, Any]) -> str:
        """Formats bugs and support section."""
        return """| Priority        | Week Current Count | Week Previous Count | Change          |
| --------------- | -----------------: | ------------------: | --------------: |
| Critical [P1]   | {p1_current}       | {p1_previous}       | {p1_change}     |
| High [P2]       | {p2_current}       | {p2_previous}       | {p2_change}     |
| Medium [P3]     | {p3_current}       | {p3_previous}       | {p3_change}     |""".format(
            p1_current=data.get("p1_current", "0"),
            p1_previous=data.get("p1_previous", "0"),
            p1_change=data.get("p1_change", "➡️ 0"),
            p2_current=data.get("p2_current", "0"),
            p2_previous=data.get("p2_previous", "0"),
            p2_change=data.get("p2_change", "➡️ 0"),
            p3_current=data.get("p3_current", "0"),
            p3_previous=data.get("p3_previous", "0"),
            p3_change=data.get("p3_change", "➡️ 0"),
        )

    def _format_cycle_time_section(self, data: dict[str, Any]) -> str:
        """Formats cycle time section."""
        return f"""| Metric               | Value (hours)       |
| -------------------- | -------------------:|
| Average Cycle Time   | {data.get("average_hours", "N/A")} h |
| Median Cycle Time    | {data.get("median_hours", "N/A")} h  |
| Max Cycle Time       | {data.get("max_hours", "N/A")} h     |"""

    def _format_adherence_section(self, data: dict[str, Any]) -> str:
        """Formats adherence section."""
        return f"""| Category     | Count | Percentage |
| ------------ | -----:| ----------:|
| Early        | {data.get("early_count", 0)}   | {data.get("early_pct", "0.0%")}     |
| On Time      | {data.get("ontime_count", 0)}   | {data.get("ontime_pct", "0.0%")}     |
| Late         | {data.get("late_count", 0)}   | {data.get("late_pct", "0.0%")}     |
| No Due Date  | {data.get("no_due_count", 0)}   | {data.get("no_due_pct", "0.0%")}     |
| **Total**    | {data.get("total_count", 0)}   | 100%       |"""

    def _format_linearb_section(self, data: dict[str, Any]) -> str:
        """Formats LinearB section."""
        return f"""| Metric                  | Week Current | Week Previous | Change          |
| ----------------------- | ------------: | ------------: | --------------: |
| Cycle Time (avg, hours) | {data.get("cycle_time_current", "N/A")} | {data.get("cycle_time_previous", "N/A")} | {data.get("cycle_time_change", "N/A")} |
| Pickup Time (avg, hours)| {data.get("pickup_time_current", "N/A")} | {data.get("pickup_time_previous", "N/A")} | {data.get("pickup_time_change", "N/A")} |
| Review Time (avg, hours)| {data.get("review_time_current", "N/A")} | {data.get("review_time_previous", "N/A")} | {data.get("review_time_change", "N/A")} |
| Deploy Frequency (count)| {data.get("deploy_freq_current", "N/A")} | {data.get("deploy_freq_previous", "N/A")} | {data.get("deploy_freq_change", "N/A")} |"""

    def _format_sonarqube_section(self, data: dict[str, Any]) -> str:
        """Formats SonarQube section."""
        return f"""| Project Name | Quality Gate | Coverage | Bugs | Reliability | Code Smells | Security Hotspots |
| ------------ | ------------ | -------- | ---- | ----------- | ----------- | ----------------- |
| Main Project | {data.get("quality_gate", "UNKNOWN")} | {data.get("coverage", "N/A%")} | {data.get("bugs", "N/A")} | {data.get("reliability", "N/A")} | {data.get("code_smells", "N/A")} | {data.get("security_hotspots", "N/A")} |

**Weekly Health Status:** {data.get("health_status", "UNKNOWN")}"""
