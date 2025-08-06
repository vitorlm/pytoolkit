"""
MCP Resources package for PyToolkit Integration.

This package provides MCP Resource handlers that aggregate data from multiple sources
to create comprehensive resources for team management and engineering analysis.
"""

from .base_resource import BaseResourceHandler
from .pipeline_resources import PipelineResourceHandler
from .quality_metrics_resources import QualityMetricsResourceHandler
from .team_metrics_resources import TeamMetricsResourceHandler
from .weekly_report_resources import WeeklyReportResourceHandler


__all__ = [
    "BaseResourceHandler",
    "TeamMetricsResourceHandler",
    "QualityMetricsResourceHandler",
    "PipelineResourceHandler",
    "WeeklyReportResourceHandler",
]
