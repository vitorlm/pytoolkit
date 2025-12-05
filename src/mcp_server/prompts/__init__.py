"""MCP Prompts package for PyToolkit Integration.

This package provides MCP Prompt handlers that generate specialized prompts
for management, reporting, and analysis tasks.
"""

from .base_prompt import BasePromptHandler
from .quality_report_prompts import QualityReportPromptHandler
from .quarterly_review_prompts import QuarterlyReviewPromptHandler
from .team_performance_prompts import TeamPerformancePromptHandler
from .weekly_report_prompts import WeeklyReportPromptHandler

__all__ = [
    "BasePromptHandler",
    "QualityReportPromptHandler",
    "QuarterlyReviewPromptHandler",
    "TeamPerformancePromptHandler",
    "WeeklyReportPromptHandler",
]
