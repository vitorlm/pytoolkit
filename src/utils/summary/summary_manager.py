"""Abstract base class for standardized summary metrics management.

This module provides the foundation for centralized summary generation across
different domains in PyToolkit, eliminating code duplication and ensuring
consistent metric reporting.
"""

import os
from abc import ABC, abstractmethod
from argparse import Namespace
from typing import Any

from utils.data.json_manager import JSONManager
from utils.logging.logging_manager import LogManager


class SummaryManager(ABC):
    """Abstract base class for domain-specific summary management.

    This class provides standardized summary generation infrastructure while
    allowing domain-specific customization through abstract methods.
    """

    def __init__(self, domain_name: str):
        """Initialize the SummaryManager.

        Args:
            domain_name: Name of the domain (e.g., 'jira', 'datadog')
        """
        self.domain_name = domain_name
        self.logger = LogManager.get_instance().get_logger(f"{domain_name}SummaryManager")

    def emit_summary(self, args: Namespace, data: Any, base_filename: str, output_dir: str = "output") -> None:
        """Generate and emit summary based on the specified mode.

        Args:
            args: Command arguments containing summary configuration
            data: Domain-specific data to summarize
            base_filename: Base filename for output files
            output_dir: Output directory for files
        """
        try:
            # Determine summary mode
            summary_mode = getattr(args, "summary", "auto")

            if summary_mode == "none":
                self.logger.info("Summary generation disabled")
                return

            # Build metrics
            metrics = self.build_metrics(data, args)

            if summary_mode == "json" or summary_mode == "auto":
                self._write_summary_json(metrics, base_filename, output_dir)

            if summary_mode == "auto":
                self._print_summary_console(metrics)

        except Exception as e:
            self.logger.error(f"Failed to emit summary: {e}", exc_info=True)
            raise

    @abstractmethod
    def build_metrics(self, data: Any, args: Namespace) -> dict[str, Any]:
        """Build domain-specific metrics from the provided data.

        Args:
            data: Domain-specific data to analyze
            args: Command arguments for context

        Returns:
            Dictionary containing structured metrics
        """
        pass

    def _write_summary_json(self, metrics: dict[str, Any], base_filename: str, output_dir: str) -> None:
        """Write summary metrics to JSON file."""
        try:
            os.makedirs(output_dir, exist_ok=True)
            summary_filename = f"{base_filename}_summary.json"
            summary_path = os.path.join(output_dir, summary_filename)

            JSONManager.write_json(metrics, summary_path)
            self.logger.info(f"Summary written to {summary_path}")

        except Exception as e:
            self.logger.error(f"Failed to write summary JSON: {e}", exc_info=True)
            raise

    def _print_summary_console(self, metrics: dict[str, Any]) -> None:
        """Print summary metrics to console."""
        try:
            print("\n" + "=" * 50)
            print("SUMMARY")
            print("=" * 50)

            # Print period and base dimensions
            if "period" in metrics:
                print(f"Period: {metrics['period']}")

            if "dimensions" in metrics:
                for key, value in metrics["dimensions"].items():
                    if value:  # Only print non-empty values
                        print(f"{key.replace('_', ' ').title()}: {value}")

            # Print key metrics
            if "metrics" in metrics:
                print("\nKey Metrics:")
                for metric in metrics["metrics"]:
                    name = metric.get("name", "Unknown")
                    value = metric.get("value", "N/A")
                    unit = metric.get("unit", "")
                    print(f"  {name}: {value} {unit}".strip())

            print("=" * 50 + "\n")

        except Exception as e:
            self.logger.error(f"Failed to print summary: {e}", exc_info=True)
            raise

    def _build_base_dimensions(self, args: Namespace) -> dict[str, Any]:
        """Build base dimensions common across domains.

        Args:
            args: Command arguments

        Returns:
            Dictionary with base dimension information
        """
        dimensions = {}

        # Common argument mappings
        arg_mappings = {
            "time_window": "time_window",
            "team": "team",
            "project_key": "project_key",
            "squad": "squad",
            "issue_types": "issue_types",
        }

        for arg_name, dim_name in arg_mappings.items():
            value = getattr(args, arg_name, None)
            if value:
                dimensions[dim_name] = value

        return dimensions

    def append_metric_safe(
        self,
        metrics_list: list[dict[str, Any]],
        name: str,
        value: Any,
        unit: str = "",
        description: str = "",
    ) -> None:
        """Safely append a metric to the metrics list with validation.

        Args:
            metrics_list: List to append the metric to
            name: Metric name
            value: Metric value
            unit: Unit of measurement (optional)
            description: Metric description (optional)
        """
        try:
            # Validate inputs
            if not name:
                self.logger.warning("Attempted to add metric with empty name")
                return

            # Convert value to appropriate type
            if value is None:
                value = "N/A"
            elif isinstance(value, (int, float)) and value != value:  # Check for NaN
                value = "N/A"

            metric = {
                "name": str(name),
                "value": value,
                "unit": str(unit) if unit else "",
                "description": str(description) if description else "",
            }

            metrics_list.append(metric)

        except Exception as e:
            self.logger.error(f"Failed to append metric '{name}': {e}", exc_info=True)
