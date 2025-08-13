"""
Base Resource Handler for MCP Integration with PyToolkit.

This module provides the base resource handler class that all resource handlers must inherit from.
It provides common functionality for aggregating data from multiple sources with robust error handling.
"""

import json
from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import datetime
from typing import Any

from mcp.types import Resource, TextResourceContents

from utils.cache_manager.cache_manager import CacheManager
from utils.logging.logging_manager import LogManager


class BaseResourceHandler(ABC):
    """
    Base handler for MCP resources that aggregate data from multiple sources.

    Provides:
    - Optimized cache for heavy resources
    - Standardized data aggregation
    - Error handling for partial failures
    - Structured logging
    - Support for quarters/cycles structure (Q1 C1, Q1 C2, etc.)
    """

    def __init__(self, resource_name: str):
        """
        Initializes base handler.

        Args:
            resource_name: Resource name for logging
        """
        self.resource_name = resource_name
        self.logger = LogManager.get_instance().get_logger("MCPResource", resource_name)
        self.cache = CacheManager.get_instance()

        self.logger.info(f"Initializing {resource_name} resource handler")

    @abstractmethod
    def get_resource_definitions(self) -> list[Resource]:
        """Returns resource definitions."""

    @abstractmethod
    async def get_resource_content(self, uri: str) -> TextResourceContents:
        """Gets content of a specific resource."""

    def get_cache_key(self, operation: str, **kwargs) -> str:
        """Generates cache key for resources."""
        params = "_".join([f"{k}_{v}" for k, v in sorted(kwargs.items())])
        return f"resource_{self.resource_name}_{operation}_{params}"

    def cached_resource_operation(
        self, operation: str, func, expiration_minutes: int = 120, **kwargs
    ) -> Any:
        """
        Executes resource operation with long cache (resources are heavier).

        Args:
            operation: Operation name
            func: Function to be executed
            expiration_minutes: Cache time (default: 2 hours)
            **kwargs: Parameters for the function
        """
        cache_key = self.get_cache_key(operation, **kwargs)

        # Try to load from cache
        cached_result = self.cache.load(
            cache_key, expiration_minutes=expiration_minutes
        )
        if cached_result is not None:
            self.logger.debug(f"Resource cache hit for {operation}")
            return cached_result

        try:
            # Execute operation
            self.logger.info(f"Generating resource {operation} - cache miss")
            result = func(**kwargs)

            # Save to cache
            self.cache.save(cache_key, result)
            self.logger.info(f"Cached resource result for {operation}")

            return result

        except Exception as e:
            self.logger.error(f"Error generating resource {operation}: {e}")
            raise

    def aggregate_data_safely(
        self,
        data_sources: dict[str, Callable],
        required_sources: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Aggregates data from multiple sources with partial failure handling.

        Args:
            data_sources: dict with source name and function to get data
            required_sources: Required sources (fail if they don't work)
        """
        # Create explicit dictionaries for type clarity
        sources: dict[str, Any] = {}
        errors: dict[str, str] = {}

        aggregated_data: dict[str, Any] = {
            "timestamp": datetime.now().isoformat(),
            "sources": sources,
            "errors": errors,
            "status": "success",
        }

        required_sources = required_sources or []

        for source_name, fetch_func in data_sources.items():
            try:
                self.logger.debug(f"Fetching data from {source_name}")
                data = fetch_func()
                sources[source_name] = data
                self.logger.debug(f"Successfully fetched data from {source_name}")

            except Exception as e:
                error_msg = f"Failed to fetch data from {source_name}: {str(e)}"
                self.logger.warning(error_msg)
                errors[source_name] = error_msg

                # If it's a required source, fail completely
                if source_name in required_sources:
                    aggregated_data["status"] = "failed"
                    raise Exception(f"Required source {source_name} failed: {e}")

        # If we have errors but they're not from required sources, mark as partial
        if errors and aggregated_data["status"] == "success":
            aggregated_data["status"] = "partial"

        return aggregated_data

    def format_resource_content(
        self, data: dict[str, Any], title: str, description: str
    ) -> str:
        """Formats resource content in a standardized way."""
        formatted_content = f"""# {title}

{description}

**Generated:** {data.get("timestamp", "Unknown")}
**Status:** {data.get("status", "Unknown")}

## Data Sources
"""

        # list data sources
        for source_name, source_data in data.get("sources", {}).items():
            formatted_content += f"\n### {source_name}\n"
            if isinstance(source_data, dict):
                formatted_content += (
                    f"```json\n{json.dumps(source_data, indent=2)}\n```\n"
                )
            else:
                formatted_content += f"{source_data}\n"

        # List errors if any
        if data.get("errors"):
            formatted_content += "\n## Errors\n"
            for source_name, error in data["errors"].items():
                formatted_content += f"- **{source_name}**: {error}\n"

        return formatted_content

    def parse_quarter_cycle(self, period: str) -> dict[str, Any]:
        """
        Analyzes period in format Q1-C1, Q2-C2, etc.

        Args:
            period: Period in format "Q1-C1" or "current" for current period

        Returns:
            dict with quarter and cycle information
        """
        if period.lower() == "current":
            # Determine current quarter/cycle based on date
            current_date = datetime.now()
            quarter = ((current_date.month - 1) // 3) + 1

            # Simplification: considers first half of quarter as C1, second as C2
            days_in_quarter = current_date.day + ((current_date.month - 1) % 3) * 30
            cycle = 1 if days_in_quarter <= 45 else 2

            return {
                "quarter": quarter,
                "cycle": cycle,
                "period_code": f"Q{quarter}-C{cycle}",
                "is_current": True,
            }

        # Parse format Q1-C1, Q2-C2, etc.
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
            self.logger.warning(
                f"Invalid period format '{period}', using current period"
            )
            return self.parse_quarter_cycle("current")

    def get_period_days(self, _quarter: int, _cycle: int) -> int:
        """
        Returns approximate number of days for a quarter/cycle.

        Args:
            quarter: Quarter number (1-4)
            cycle: Cycle number (1-2)

        Returns:
            Approximate number of days
        """
        # Each quarter has ~90 days, each cycle ~45 days
        return 45

    def format_quarter_cycle_summary(self, period_info: dict[str, Any]) -> str:
        """Formats quarter/cycle period summary."""
        return f"**Period:** {period_info['period_code']} (Quarter {period_info['quarter']}, Cycle {period_info['cycle']})"
