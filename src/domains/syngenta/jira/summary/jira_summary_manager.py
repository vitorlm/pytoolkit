"""
JIRA-specific summary metrics management.

This module provides JIRA domain-specific implementation of the SummaryManager,
handling cycle time, issue adherence, and net flow metrics with full compatibility
to existing command structures.
"""

import os
from argparse import Namespace
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from utils.summary.summary_manager import SummaryManager
from utils.summary_helpers import (
    _extract_metric_value,
    _has_value,
    _isoz,
    build_standard_period,
)
from utils.output_manager import OutputManager


class JiraSummaryManager(SummaryManager):
    """
    JIRA-specific summary metrics management.

    Handles all JIRA command summary generation with support for:
    - Cycle time metrics
    - Issue adherence metrics
    - Net flow metrics
    - Team segmentation
    - Trending analysis
    """

    def __init__(self):
        super().__init__("jira")

    def build_metrics(self, data: Any, args: Namespace) -> Dict[str, Any]:
        """
        Build JIRA-specific metrics from command result data.

        Args:
            data: JIRA command result dictionary
            args: Command arguments for context

        Returns:
            Dictionary containing structured JIRA metrics
        """
        try:
            # Extract components from JIRA result structure
            result = data if isinstance(data, dict) else {}
            metadata = result.get("analysis_metadata", {})
            metrics_block = result.get("metrics", {})

            # Build period information
            period_info = self._build_period_info(metadata, args)

            # Build base dimensions
            dimensions = self._build_jira_dimensions(metadata, args)

            # Build metrics list
            metrics_list = self._build_jira_metrics_list(
                result, metrics_block, period_info, dimensions, args
            )

            return {
                "period": period_info,
                "dimensions": dimensions,
                "metrics": metrics_list,
                "source_command": getattr(args, "command_name", "jira-unknown"),
                "generated_at": datetime.now().isoformat(),
                "raw_data_available": bool(getattr(args, "output_file", None)),
            }

        except Exception as e:
            self.logger.error(f"Failed to build JIRA metrics: {e}", exc_info=True)
            return {
                "error": str(e),
                "period": {"description": "Error occurred"},
                "dimensions": {},
                "metrics": [],
            }

    def emit_summary_compatible(
        self,
        result: Dict[str, Any],
        summary_mode: str,
        existing_output_path: Optional[str],
        args: Namespace,
    ) -> Optional[str]:
        """
        Emit summary with full compatibility to existing JIRA command structure.

        This method provides drop-in replacement for existing _emit_summary methods.

        Args:
            result: JIRA command result dictionary
            summary_mode: Summary mode ('auto', 'json', 'none')
            existing_output_path: Path to existing output file
            args: Command arguments

        Returns:
            Path to generated summary file or None
        """
        try:
            if summary_mode == "none":
                return None

            # Get raw data path for compatibility
            raw_data_path = (
                os.path.abspath(existing_output_path) if existing_output_path else None
            )

            # Build metrics using legacy format for compatibility
            metrics_payload = self._build_legacy_summary_metrics(
                result, raw_data_path, args
            )
            if not metrics_payload:
                return None

            # Use existing output patterns
            sub_dir, base_name = self._get_output_defaults(args, result)
            summary_path: Optional[str] = None

            if existing_output_path:
                target_path = self._get_summary_path_for_existing(existing_output_path)
                summary_path = OutputManager.save_summary_report(
                    metrics_payload,
                    sub_dir,
                    base_name,
                    output_path=target_path,
                )
                summary_path = os.path.abspath(summary_path)

            if summary_mode == "auto":
                return summary_path

            if summary_path and summary_mode == "json":
                return summary_path

            if summary_mode == "json":
                summary_path = OutputManager.save_summary_report(
                    metrics_payload,
                    sub_dir,
                    base_name,
                )
                return os.path.abspath(summary_path)

            return None

        except Exception as e:
            self.logger.error(f"Failed to emit compatible summary: {e}", exc_info=True)
            return None

    def _build_period_info(
        self, metadata: Dict[str, Any], args: Namespace
    ) -> Dict[str, Any]:
        """Build period information from metadata and args."""
        period_start = _isoz(metadata.get("start_date"))
        period_end = _isoz(metadata.get("end_date"))

        if period_start and period_end:
            return {
                "start_date": period_start,
                "end_date": period_end,
                "description": f"Period: {period_start} to {period_end}",
            }

        # Fallback to args-based period
        time_window = getattr(args, "time_window", None)
        if time_window:
            return build_standard_period(time_window)

        return {"description": "Period not specified"}

    def _build_jira_dimensions(
        self, metadata: Dict[str, Any], args: Namespace
    ) -> Dict[str, Any]:
        """Build JIRA-specific dimensions."""
        dimensions = {}

        # Project key
        project_key = metadata.get("project_key") or getattr(args, "project_key", None)
        if project_key:
            dimensions["project"] = project_key

        # Team information (with normalization)
        team_value = self._normalize_team_metadata(metadata)
        dimensions["team"] = team_value or "overall"

        # Issue types
        issue_types = metadata.get("issue_types") or getattr(args, "issue_types", None)
        if issue_types:
            if isinstance(issue_types, list):
                dimensions["issue_types"] = ", ".join(issue_types)
            else:
                dimensions["issue_types"] = str(issue_types)

        # Additional args-based dimensions
        for field in ["squad", "operation", "verbose"]:
            value = getattr(args, field, None)
            if value is not None:
                dimensions[field] = value

        return dimensions

    def _build_jira_metrics_list(
        self,
        result: Dict[str, Any],
        metrics_block: Dict[str, Any],
        period_info: Dict[str, Any],
        dimensions: Dict[str, Any],
        args: Namespace,
    ) -> List[Dict[str, Any]]:
        """Build comprehensive JIRA metrics list."""
        metrics_list = []

        # Determine command type from result structure
        command_name = getattr(args, "command_name", "jira-unknown")

        # Core metrics based on command type
        if (
            "cycle_time" in str(command_name)
            or "average_cycle_time_hours" in metrics_block
        ):
            self._add_cycle_time_metrics(metrics_list, metrics_block)

        if "adherence" in str(command_name) or "adherence_stats" in metrics_block:
            self._add_adherence_metrics(metrics_list, metrics_block)

        if "net_flow" in str(command_name) or "flow_metrics" in metrics_block:
            self._add_net_flow_metrics(metrics_list, metrics_block)

        # Team segmentation metrics
        self._add_segmentation_metrics(metrics_list, result, period_info, dimensions)

        # Trending and advanced metrics
        self._add_trending_metrics(metrics_list, result)

        return metrics_list

    def _add_cycle_time_metrics(
        self, metrics_list: List[Dict[str, Any]], metrics_block: Dict[str, Any]
    ):
        """Add cycle time specific metrics."""
        cycle_time_metrics = [
            ("average_cycle_time_hours", "Average Cycle Time", "hours"),
            ("median_cycle_time_hours", "Median Cycle Time", "hours"),
            ("min_cycle_time_hours", "Minimum Cycle Time", "hours"),
            ("max_cycle_time_hours", "Maximum Cycle Time", "hours"),
            ("total_issues", "Total Issues", "issues"),
            ("p95_cycle_time_hours", "95th Percentile Cycle Time", "hours"),
            ("standard_deviation_hours", "Cycle Time Standard Deviation", "hours"),
        ]

        for metric_key, name, unit in cycle_time_metrics:
            value = metrics_block.get(metric_key)
            if _has_value(value):
                self.append_metric_safe(metrics_list, name, value, unit)

        # SLE compliance
        sle_value = _extract_metric_value(
            metrics_block, ("sle_adherence", "compliance_rate")
        )
        if _has_value(sle_value):
            self.append_metric_safe(
                metrics_list, "SLE Compliance", sle_value, "percent"
            )

    def _add_adherence_metrics(
        self, metrics_list: List[Dict[str, Any]], metrics_block: Dict[str, Any]
    ):
        """Add issue adherence specific metrics."""
        adherence_metrics = [
            ("adherence_rate", "Overall Adherence Rate", "percent"),
            ("weighted_adherence_rate", "Weighted Adherence Rate", "percent"),
            ("total_issues_analyzed", "Total Issues Analyzed", "issues"),
            ("on_time_issues", "On-Time Issues", "issues"),
            ("late_issues", "Late Issues", "issues"),
            ("avg_days_late", "Average Days Late", "days"),
        ]

        for metric_key, name, unit in adherence_metrics:
            value = metrics_block.get(metric_key)
            if _has_value(value):
                self.append_metric_safe(metrics_list, name, value, unit)

    def _add_net_flow_metrics(
        self, metrics_list: List[Dict[str, Any]], metrics_block: Dict[str, Any]
    ):
        """Add net flow specific metrics."""
        flow_metrics = [
            ("net_flow", "Net Flow", "issues"),
            ("inflow", "Inflow", "issues"),
            ("outflow", "Outflow", "issues"),
            ("throughput", "Throughput", "issues"),
            ("work_in_progress", "Work in Progress", "issues"),
            ("flow_efficiency", "Flow Efficiency", "percent"),
            ("volatility_score", "Volatility Score", "score"),
        ]

        for metric_key, name, unit in flow_metrics:
            value = metrics_block.get(metric_key)
            if _has_value(value):
                self.append_metric_safe(metrics_list, name, value, unit)

    def _add_segmentation_metrics(
        self,
        metrics_list: List[Dict[str, Any]],
        result: Dict[str, Any],
        period_info: Dict[str, Any],
        dimensions: Dict[str, Any],
    ):
        """Add team/type segmentation metrics."""
        # Extract segmentation data
        segments = result.get("segments", {})
        if not segments:
            return

        for segment_name, segment_data in segments.items():
            segment_metrics = segment_data.get("metrics", {})

            # Add key metrics for each segment
            for metric_key in [
                "average_cycle_time_hours",
                "median_cycle_time_hours",
                "total_issues",
            ]:
                value = segment_metrics.get(metric_key)
                if _has_value(value):
                    metric_name = (
                        f"{segment_name} - {metric_key.replace('_', ' ').title()}"
                    )
                    unit = "hours" if "time" in metric_key else "issues"
                    self.append_metric_safe(metrics_list, metric_name, value, unit)

    def _add_trending_metrics(
        self, metrics_list: List[Dict[str, Any]], result: Dict[str, Any]
    ):
        """Add trending and comparative metrics."""
        trending = result.get("trending", {})
        if not trending:
            return

        trend_metrics = [
            ("baseline_comparison", "Baseline Comparison", "percent"),
            ("trend_direction", "Trend Direction", "direction"),
            ("volatility", "Trend Volatility", "score"),
            ("alert_threshold_breached", "Alert Threshold Breached", "boolean"),
        ]

        for metric_key, name, unit in trend_metrics:
            value = trending.get(metric_key)
            if _has_value(value):
                self.append_metric_safe(metrics_list, name, value, unit)

    def _build_legacy_summary_metrics(
        self, result: Dict[str, Any], raw_data_path: Optional[str], args: Namespace
    ) -> List[Dict[str, Any]]:
        """
        Build summary metrics in legacy format for full compatibility.

        This maintains the exact structure expected by existing integrations.
        """
        metadata = result.get("analysis_metadata") or {}
        period_start = _isoz(metadata.get("start_date"))
        period_end = _isoz(metadata.get("end_date"))

        if not period_start or not period_end:
            return []

        period = {"start_date": period_start, "end_date": period_end}
        base_dimensions = self._legacy_base_dimensions(metadata)
        metrics_block = result.get("metrics") or {}

        summary_metrics: List[Dict[str, Any]] = []
        command_name = getattr(args, "command_name", "jira-command")

        # Core metrics in legacy format
        self._append_legacy_metric(
            summary_metrics,
            "jira.cycle_time.average_hours",
            metrics_block.get("average_cycle_time_hours"),
            "hours",
            period,
            base_dimensions,
            command_name,
            raw_data_path,
        )

        self._append_legacy_metric(
            summary_metrics,
            "jira.cycle_time.median_hours",
            metrics_block.get("median_cycle_time_hours"),
            "hours",
            period,
            base_dimensions,
            command_name,
            raw_data_path,
        )

        self._append_legacy_metric(
            summary_metrics,
            "jira.cycle_time.throughput",
            metrics_block.get("total_issues"),
            "issues",
            period,
            base_dimensions,
            command_name,
            raw_data_path,
        )

        # SLE compliance
        sle_value = _extract_metric_value(
            metrics_block, ("sle_adherence", "compliance_rate")
        )
        self._append_legacy_metric(
            summary_metrics,
            "jira.cycle_time.sle_compliance_percent",
            sle_value,
            "percent",
            period,
            base_dimensions,
            command_name,
            raw_data_path,
        )

        # Team segmentation (if available)
        segments = result.get("segments", {})
        for segment_name, segment_data in segments.items():
            segment_metrics = segment_data.get("metrics", {})
            segment_dimensions = {**base_dimensions, "team": segment_name}

            self._append_legacy_metric(
                summary_metrics,
                "jira.cycle_time.average_hours",
                segment_metrics.get("average_cycle_time_hours"),
                "hours",
                period,
                segment_dimensions,
                command_name,
                raw_data_path,
            )

        return summary_metrics

    def _append_legacy_metric(
        self,
        container: List[Dict[str, Any]],
        metric_name: str,
        value: Any,
        unit: str,
        period: Dict[str, str],
        dimensions: Dict[str, Any],
        source_command: str,
        raw_data_path: Optional[str],
    ) -> None:
        """Append metric in legacy format for compatibility."""
        if not _has_value(value):
            return

        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            return

        cleaned_dimensions = {
            k: v for k, v in dimensions.items() if _has_value(v) and str(v).strip()
        }

        container.append(
            {
                "metric_name": metric_name,
                "value": numeric_value,
                "unit": unit,
                "period": period,
                "dimensions": cleaned_dimensions,
                "source_command": source_command,
                "raw_data_path": raw_data_path,
            }
        )

    def _legacy_base_dimensions(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Build base dimensions in legacy format."""
        project_key = metadata.get("project_key")
        dimensions: Dict[str, Any] = {}
        if project_key:
            dimensions["project"] = project_key

        team_value = self._normalize_team_metadata(metadata)
        dimensions["team"] = team_value or "overall"
        return dimensions

    def _normalize_team_metadata(self, metadata: Dict[str, Any]) -> Optional[str]:
        """Normalize team metadata from various sources."""
        team_candidates = [
            metadata.get("team"),
            metadata.get("teams"),
            metadata.get("squad"),
        ]

        for candidate in team_candidates:
            if candidate:
                if isinstance(candidate, list):
                    return ",".join(str(t) for t in candidate)
                return str(candidate)

        return None

    def _get_output_defaults(
        self, args: Namespace, result: Dict[str, Any]
    ) -> Tuple[str, str]:
        """Get output directory and filename defaults."""
        metadata = result.get("analysis_metadata") or {}
        project_key = metadata.get("project_key") or getattr(
            args, "project_key", "unknown"
        )
        date_str = datetime.now().strftime("%Y%m%d")

        # Determine command type for subdirectory
        command_name = getattr(args, "command_name", "jira-command")
        if "cycle" in command_name:
            sub_dir = f"cycle-time_{date_str}"
            base_name = f"cycle_time_summary_{project_key}"
        elif "adherence" in command_name:
            sub_dir = f"adherence_{date_str}"
            base_name = f"adherence_summary_{project_key}"
        elif "flow" in command_name:
            sub_dir = f"net-flow_{date_str}"
            base_name = f"net_flow_summary_{project_key}"
        else:
            sub_dir = f"jira_{date_str}"
            base_name = f"jira_summary_{project_key}"

        return sub_dir, base_name

    def _get_summary_path_for_existing(self, existing_output_path: str) -> str:
        """Generate summary path based on existing output path."""
        output_path = Path(existing_output_path)
        summary_filename = f"{output_path.stem}_summary.json"
        return str(output_path.with_name(summary_filename))
