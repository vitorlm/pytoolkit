"""Datadog-specific summary metrics management.

This module provides the Datadog domain implementation for centralized summary
generation, removing duplication from command files while preserving backward
compatible summary JSON output formats used by existing automation.
"""

from __future__ import annotations

import os
from argparse import Namespace
from datetime import datetime
from pathlib import Path
from typing import Any

from utils.output_manager import OutputManager
from utils.summary.summary_manager import SummaryManager
from utils.summary_helpers import _has_value, _isoz


class DatadogSummaryManager(SummaryManager):
    """Datadog-specific summary manager.

    Supports legacy-compatible emission used by events_command while also
    providing a generic metrics builder for potential future use.
    """

    def __init__(self) -> None:
        super().__init__("datadog")

    # --- Generic API (not currently used by events_command) ---
    def build_metrics(self, data: Any, args: Namespace) -> dict[str, Any]:
        """Build a generic Datadog metrics document. Currently minimal because
        events_command consumes the legacy-compatible emitter.
        """
        summary = data.get("summary") if isinstance(data, dict) else {}
        time_period = (summary or {}).get("time_period") or {}
        period = {
            "start_date": _isoz(time_period.get("start")) if isinstance(time_period, dict) else None,
            "end_date": _isoz(time_period.get("end")) if isinstance(time_period, dict) else None,
            "description": (time_period.get("label") if isinstance(time_period, dict) else None)
            or "Datadog analysis period",
        }

        return {
            "period": period,
            "dimensions": {
                "env": (summary or {}).get("env"),
                "teams": ",".join(
                    (summary or {}).get("requested_teams", [])
                    if isinstance((summary or {}).get("requested_teams"), list)
                    else []
                ),
            },
            "metrics": [],
            "source_command": getattr(args, "command_name", "events"),
            "generated_at": datetime.now().isoformat(),
        }

    # --- Legacy-compatible API used by events_command ---
    def emit_summary_compatible(
        self,
        payload: dict[str, Any],
        summary_mode: str,
        existing_output_path: str | None,
        teams: list[str],
    ) -> str | None:
        """Emit summary JSON with full backward compatibility to the original
        events_command implementation.

        Returns the absolute path of the written summary or None.
        """
        try:
            if summary_mode == "none":
                return None

            raw_data_path = os.path.abspath(existing_output_path) if existing_output_path else None

            metrics_payload = self._build_legacy_summary_metrics(payload, raw_data_path, teams)
            if not metrics_payload:
                return None

            sub_dir, base_name = self._output_defaults(payload, teams)
            summary_path: str | None = None

            if existing_output_path:
                target_path = self._summary_path_for_existing(existing_output_path)
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
            self.logger.error(f"Failed to emit Datadog summary (compatible): {e}", exc_info=True)
            return None

    def _build_legacy_summary_metrics(
        self, payload: dict[str, Any], raw_data_path: str | None, teams: list[str]
    ) -> list[dict[str, Any]]:
        summary_obj = payload.get("summary") or {}
        summary: dict[str, Any] = summary_obj if isinstance(summary_obj, dict) else {}
        time_period_obj = summary.get("time_period") or {}
        time_period: dict[str, Any] = time_period_obj if isinstance(time_period_obj, dict) else {}
        period_start = _isoz(time_period.get("start"))
        period_end = _isoz(time_period.get("end"))
        if not period_start or not period_end:
            return []

        period = {"start_date": period_start, "end_date": period_end}
        base_dimensions = self._base_dimensions(summary, teams)
        metrics: list[dict[str, Any]] = []
        command_name = "events"

        advanced = payload.get("advanced_analysis")
        if isinstance(advanced, dict):
            alert_quality = advanced.get("alert_quality")
            overall_quality: dict[str, Any] = {}
            if isinstance(alert_quality, dict):
                _ov = alert_quality.get("overall")
                if isinstance(_ov, dict):
                    overall_quality = _ov
            self._append_legacy_metric(
                metrics,
                "datadog.events.quality.overall_noise_score",
                overall_quality.get("overall_noise_score"),
                "score",
                period,
                base_dimensions,
                command_name,
                raw_data_path,
            )
            self._append_legacy_metric(
                metrics,
                "datadog.events.quality.self_healing_rate",
                self._percent_value(overall_quality.get("self_healing_rate")),
                "percent",
                period,
                base_dimensions,
                command_name,
                raw_data_path,
            )
            self._append_legacy_metric(
                metrics,
                "datadog.events.quality.actionable_alerts_percent",
                self._percent_value(overall_quality.get("actionable_alerts_percentage")),
                "percent",
                period,
                base_dimensions,
                command_name,
                raw_data_path,
            )

            temporal_metrics: dict[str, Any] = {}
            if isinstance(advanced, dict):
                _tm = advanced.get("temporal_metrics")
                if isinstance(_tm, dict):
                    temporal_metrics = _tm
            if isinstance(temporal_metrics, dict):
                self._append_legacy_metric(
                    metrics,
                    "datadog.events.temporal.avg_ttr_minutes",
                    temporal_metrics.get("avg_time_to_resolution_minutes"),
                    "minutes",
                    period,
                    base_dimensions,
                    command_name,
                    raw_data_path,
                )
                self._append_legacy_metric(
                    metrics,
                    "datadog.events.temporal.mtbf_hours",
                    temporal_metrics.get("mtbf_hours"),
                    "hours",
                    period,
                    base_dimensions,
                    command_name,
                    raw_data_path,
                )

            detailed_stats: dict[str, Any] = {}
            if isinstance(advanced, dict):
                _ds = advanced.get("detailed_monitor_statistics")
                if isinstance(_ds, dict):
                    detailed_stats = _ds

            overall_insights: dict[str, Any] = {}
            _oi = detailed_stats.get("overall_insights") if isinstance(detailed_stats, dict) else None
            if isinstance(_oi, dict):
                overall_insights = _oi
            self._append_legacy_metric(
                metrics,
                "datadog.events.health.average_score",
                overall_insights.get("average_health_score"),
                "score",
                period,
                base_dimensions,
                command_name,
                raw_data_path,
            )
            self._append_legacy_metric(
                metrics,
                "datadog.events.health.monitors_needing_attention",
                overall_insights.get("monitors_needing_attention"),
                "monitors",
                period,
                base_dimensions,
                command_name,
                raw_data_path,
            )

        return metrics

    def _append_legacy_metric(
        self,
        container: list[dict[str, Any]],
        metric_name: str,
        value: Any,
        unit: str,
        period: dict[str, str],
        dimensions: dict[str, Any],
        source_command: str,
        raw_data_path: str | None,
    ) -> None:
        if not _has_value(value):
            return

        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            return

        cleaned_dimensions = {k: v for k, v in dimensions.items() if _has_value(v) and str(v).strip()}
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

    def _base_dimensions(self, summary: dict[str, Any], teams: list[str]) -> dict[str, Any]:
        dimensions: dict[str, Any] = {}
        env = summary.get("env") if isinstance(summary, dict) else None
        if env:
            dimensions["env"] = env

        team_dimension = self._format_team_dimension(summary, teams)
        dimensions["team"] = team_dimension or "overall"
        return dimensions

    def _format_team_dimension(self, summary: dict[str, Any], teams: list[str]) -> str | None:
        candidates: list[str] = []
        requested = summary.get("requested_teams") if isinstance(summary, dict) else None
        if isinstance(requested, list):
            candidates.extend(str(team).strip() for team in requested if str(team).strip())

        candidates.extend(str(team).strip() for team in teams if str(team).strip())

        unique: list[str] = []
        seen = set()
        for team in candidates:
            if team not in seen:
                seen.add(team)
                unique.append(team)

        if unique:
            return ",".join(unique)
        return None

    def _percent_value(self, value: Any) -> float | None:
        if not _has_value(value):
            return None

        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            return None

        scaled_value = numeric_value * 100 if numeric_value <= 1.0 else numeric_value
        return round(scaled_value, 2)

    def _output_defaults(self, payload: dict[str, object], teams: list[str]) -> tuple[str, str]:
        summary = payload.get("summary") or {}
        env = summary.get("env") if isinstance(summary, dict) else None
        env_label = env or "all"
        date_str = datetime.now().strftime("%Y%m%d")
        sub_dir = f"datadog-events_{date_str}"
        base_name = f"datadog_events_summary_{env_label}"
        if teams:
            base_name += f"_{len(teams)}teams"
        return sub_dir, base_name

    def _summary_path_for_existing(self, existing_output_path: str) -> str:
        output_path = Path(existing_output_path)
        summary_filename = f"{output_path.stem}_summary.json"
        return str(output_path.with_name(summary_filename))
