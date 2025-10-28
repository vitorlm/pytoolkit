"""
JIRA Net Flow Calculation Service

This service calculates net flow metrics by analyzing arrival rate (issues created)
versus throughput (issues completed) for specified time periods. It supports
generating a scorecard with a rolling 4-week trend analysis.

Net Flow = Arrival Rate - Throughput

- Positive Net Flow: More work is arriving than being completed (backlog may be growing).
- Negative Net Flow: More work is being completed than arriving (backlog may be shrinking).
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from collections import defaultdict
import math
import random
import re
import numpy as np
from enum import Enum

from domains.syngenta.jira.issue_adherence_service import TimePeriodParser
from domains.syngenta.jira.workflow_config_service import WorkflowConfigService
from utils.data.json_manager import JSONManager
from utils.jira.jira_assistant import JiraAssistant
from utils.logging.logging_manager import LogManager
from utils.output_manager import OutputManager


@dataclass
class WeeklyFlowMetrics:
    """Data class to hold flow metrics for a single week."""

    week_number: int
    start_date: str
    end_date: str
    arrival_rate: int
    throughput: int
    net_flow: int


@dataclass
class StatisticalSignal:
    """Statistical signal analysis result."""

    ci_low: float
    ci_high: float
    signal_label: str
    confidence_level: float = 0.95


@dataclass
class TrendAnalysis:
    """Trend analysis result."""

    ewma_values: List[float]
    current_ewma: float
    direction: str  # ↑/↓/→
    cusum_shift_detected: bool = False
    shift_point: Optional[int] = None


@dataclass
class VolatilityMetrics:
    """Volatility and stability metrics."""

    arrivals_cv: float
    throughput_cv: float
    stability_index: float


@dataclass
class FlowAlert:
    """Flow health alert."""

    id: str
    title: str
    triggered: bool
    rationale: str
    remediation: str


@dataclass
class SegmentMetrics:
    """Segmented metrics by issue type or priority."""

    segment_name: str
    arrivals: int
    throughput: int
    net_flow: int


@dataclass
class BottleneckAnalysis:
    """Bottleneck analysis for workflow columns."""

    column_name: str
    aging_p50: float
    aging_p85: float
    aging_p95: float
    flow_efficiency: float
    current_wip: int


@dataclass
class AssignmentMetrics:
    """Assignment and ownership tracking metrics."""

    assignment_lag_avg: float  # Hours from created to first assignment
    assignment_lag_p85: float  # P85 assignment lag
    unassigned_count: int  # Current unassigned issues
    unassigned_percentage: float  # % of issues unassigned
    reassignment_frequency: float  # Avg reassignments per issue
    team_load_balance: Dict[str, int]  # Assigned issues per team member (all statuses)
    team_load_wip: Dict[str, int]  # WIP issues per team member (active statuses only)
    handoff_quality_score: float  # 0-100 based on clean handoffs


@dataclass
class StageTransition:
    """Individual workflow stage transition."""

    from_stage: str
    to_stage: str
    avg_time_hours: float
    median_time_hours: float
    p85_time_hours: float
    issue_count: int


@dataclass
class CycleTimeHeatmap:
    """Cycle time breakdown by workflow stages."""

    transitions: List[StageTransition]
    total_cycle_time: float
    bottleneck_stage: str
    efficiency_by_stage: Dict[str, float]


@dataclass
class NetFlowScorecard:
    """Data class for the complete net flow scorecard."""

    metadata: Dict
    current_week: WeeklyFlowMetrics
    rolling_trend: List[WeeklyFlowMetrics]
    insights: List[str]
    details: Dict = field(default_factory=dict)

    # New statistical fields
    statistical_signal: Optional[StatisticalSignal] = None
    trend_analysis: Optional[TrendAnalysis] = None
    volatility_metrics: Optional[VolatilityMetrics] = None
    flow_debt: int = 0
    normalized_metrics: Dict = field(default_factory=dict)
    segments: List[SegmentMetrics] = field(default_factory=list)
    bottlenecks: List[BottleneckAnalysis] = field(default_factory=list)
    alerts: List[FlowAlert] = field(default_factory=list)


class SignalLabel(Enum):
    """Statistical signal labels for Net Flow."""

    LIKELY_ACCUMULATION = "Likely accumulation"
    LIKELY_REDUCTION = "Likely reduction"
    INCONCLUSIVE = "Inconclusive/Noise"


class NetFlowCalculationService:
    """Service class for net flow calculations and scorecard generation."""

    def __init__(self):
        self.jira_assistant = JiraAssistant()
        self.workflow_service = WorkflowConfigService()
        self.time_parser = TimePeriodParser()
        self.logger = LogManager.get_instance().get_logger("NetFlowCalculationService")

        # Configure random seed for reproducible bootstrap results
        random.seed(42)
        np.random.seed(42)

    def _parse_datetime(self, value: Optional[str]) -> Optional[datetime]:
        """Safely parse ISO timestamps from JIRA responses, handling offsets without colon."""
        if not value:
            return None

        if isinstance(value, datetime):
            return value

        ts = value.strip()

        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"

        tz_component = ""
        match = re.search(r"([+-]\d{2}:\d{2})$", ts)
        if match:
            tz_component = match.group(1)
            ts = ts[: match.start()]
        else:
            match = re.search(r"([+-]\d{2})(\d{2})$", ts)
            if match:
                tz_component = f"{match.group(1)}:{match.group(2)}"
                ts = ts[: match.start()]

        ts = ts.rstrip()

        if "." in ts:
            base, frac = ts.split(".", 1)
            frac_digits = "".join(ch for ch in frac if ch.isdigit())
            if frac_digits:
                frac_digits = (frac_digits + "000000")[:6]
                ts = f"{base}.{frac_digits}"
            else:
                ts = base

        candidate = ts + tz_component

        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            pass

        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
        ):
            try:
                return datetime.strptime(candidate, fmt)
            except ValueError:
                continue

        return None

    def bootstrap_net_flow_ci(
        self,
        arrivals: int,
        throughput: int,
        B: int = 2000,
        confidence_level: float = 0.95,
    ) -> StatisticalSignal:
        """
        Compute bootstrap confidence interval for Net Flow using small-sample awareness.

        Args:
            arrivals: Number of arrivals in the period
            throughput: Number of throughput items in the period
            B: Number of bootstrap resamples (default: 2000)
            confidence_level: Confidence level (default: 0.95)

        Returns:
            StatisticalSignal with CI and signal label
        """
        if arrivals == 0 and throughput == 0:
            return StatisticalSignal(0.0, 0.0, SignalLabel.INCONCLUSIVE.value, confidence_level)

        # For very small samples, use Poisson bootstrap approximation
        net_flows = []
        for _ in range(B):
            # Bootstrap arrivals and throughput
            bootstrap_arrivals = np.random.poisson(arrivals) if arrivals > 0 else 0
            bootstrap_throughput = np.random.poisson(throughput) if throughput > 0 else 0
            net_flows.append(bootstrap_arrivals - bootstrap_throughput)

        net_flows = np.array(net_flows)

        # Calculate confidence interval using percentile method
        alpha = 1 - confidence_level
        ci_low = np.percentile(net_flows, (alpha / 2) * 100)
        ci_high = np.percentile(net_flows, (1 - alpha / 2) * 100)

        # Determine signal label
        if ci_low > 0:
            signal_label = SignalLabel.LIKELY_ACCUMULATION.value
        elif ci_high < 0:
            signal_label = SignalLabel.LIKELY_REDUCTION.value
        else:
            signal_label = SignalLabel.INCONCLUSIVE.value

        return StatisticalSignal(ci_low, ci_high, signal_label, confidence_level)

    def compute_ewma(self, series: List[float], alpha: float = 0.2) -> TrendAnalysis:
        """
        Compute Exponentially Weighted Moving Average for trend analysis.

        Args:
            series: Time series data (e.g., flow ratios)
            alpha: Smoothing parameter (default: 0.2)

        Returns:
            TrendAnalysis with EWMA values and direction
        """
        if not series:
            return TrendAnalysis([], 0.0, "→")

        ewma_values = []
        ewma = series[0]  # Initialize with first value
        ewma_values.append(ewma)

        for value in series[1:]:
            ewma = alpha * value + (1 - alpha) * ewma
            ewma_values.append(ewma)

        # Determine trend direction
        if len(ewma_values) >= 2:
            current_ewma = ewma_values[-1]
            prev_ewma = ewma_values[-2]

            if current_ewma > prev_ewma * 1.02:  # 2% threshold for upward trend
                direction = "↑"
            elif current_ewma < prev_ewma * 0.98:  # 2% threshold for downward trend
                direction = "↓"
            else:
                direction = "→"
        else:
            direction = "→"

        return TrendAnalysis(ewma_values, ewma_values[-1], direction)

    def detect_cusum_shift(self, series: List[float], k_factor: float = 0.5, h_threshold: float = 5.0) -> bool:
        """
        Detect sustained shifts using CUSUM algorithm.

        Args:
            series: Time series data
            k_factor: Reference value multiplier (default: 0.5 * sigma)
            h_threshold: Decision threshold (default: 5.0 * sigma)

        Returns:
            Boolean indicating if shift was detected
        """
        if len(series) < 3:
            return False

        # Calculate reference value (mean) and standard deviation
        mean_val = np.mean(series)
        std_val = np.std(series)

        if std_val == 0:
            return False

        k = k_factor * std_val
        h = h_threshold * std_val

        # CUSUM for upward shift
        s_plus = 0
        # CUSUM for downward shift
        s_minus = 0

        for value in series:
            s_plus = max(0, s_plus + (value - mean_val - k))
            s_minus = max(0, s_minus + (mean_val - value - k))

            if s_plus > h or s_minus > h:
                return True

        return False

    def compute_rolling_cv(self, series: List[float], window: int = 8) -> List[Optional[float]]:
        """
        Compute rolling coefficient of variation.

        Args:
            series: Time series data
            window: Rolling window size (default: 8 weeks)

        Returns:
            List of CV values (None for insufficient data points)
        """
        if len(series) < 2:
            return [None] * len(series)

        cv_values = []
        for i in range(len(series)):
            start_idx = max(0, i - window + 1)
            window_data = series[start_idx : i + 1]

            if len(window_data) < 2:
                cv_values.append(None)
                continue

            mean_val = np.mean(window_data)
            if mean_val == 0:
                cv_values.append(0.0)
            else:
                std_val = np.std(window_data)
                cv = (std_val / abs(mean_val)) * 100
                cv_values.append(cv)

        return cv_values

    def compute_stability_index(self, net_flow_series: List[int], window: int = 8) -> float:
        """
        Compute stability index as % of periods within control bands.

        Args:
            net_flow_series: Historical net flow values
            window: Window for control band calculation

        Returns:
            Stability index as percentage
        """
        if not net_flow_series:
            return 0.0

        # Use an effective window based on available history
        window_eff = min(window, len(net_flow_series))
        recent_data = net_flow_series[-window_eff:]
        mean_val = np.mean(recent_data)
        std_val = np.std(recent_data)

        if std_val == 0:
            return 100.0  # All values are the same, perfectly stable

        # Count values within ±1 sigma
        lower_bound = mean_val - std_val
        upper_bound = mean_val + std_val

        within_bounds = sum(1 for x in recent_data if lower_bound <= x <= upper_bound)
        return (within_bounds / len(recent_data)) * 100

    def compute_flow_debt(self, net_flow_history: List[int]) -> int:
        """
        Compute cumulative flow debt (positive net flows over time).

        Args:
            net_flow_history: Historical net flow values

        Returns:
            Cumulative flow debt
        """
        return sum(max(nf, 0) for nf in net_flow_history)

    def normalize_per_dev(self, value: float, active_devs: Optional[int]) -> Optional[float]:
        """
        Normalize metrics per active developer.

        Args:
            value: Value to normalize
            active_devs: Number of active developers

        Returns:
            Normalized value or None if active_devs not provided
        """
        if active_devs is None or active_devs == 0:
            return None
        return value / active_devs

    def analyze_segments_by_type(
        self, arrival_issues: List[Dict], completed_issues: List[Dict]
    ) -> List[SegmentMetrics]:
        """
        Analyze metrics segmented by issue type.

        Args:
            arrival_issues: List of arrived issues
            completed_issues: List of completed issues

        Returns:
            List of segmented metrics
        """
        # Count by issue type
        arrival_types = {}
        completion_types = {}

        for issue in arrival_issues:
            issue_type = issue.get("type", "Unknown")
            arrival_types[issue_type] = arrival_types.get(issue_type, 0) + 1

        for issue in completed_issues:
            issue_type = issue.get("type", "Unknown")
            completion_types[issue_type] = completion_types.get(issue_type, 0) + 1

        # Create segments
        all_types = set(arrival_types.keys()) | set(completion_types.keys())
        segments = []

        for issue_type in sorted(all_types):
            arrivals = arrival_types.get(issue_type, 0)
            throughput = completion_types.get(issue_type, 0)
            net_flow = arrivals - throughput

            segments.append(SegmentMetrics(issue_type, arrivals, throughput, net_flow))

        return segments

    def generate_health_alerts(
        self,
        signal: StatisticalSignal,
        trend: TrendAnalysis,
        volatility: VolatilityMetrics,
        flow_efficiency: float,
        testing_aging_p85: float = 0.0,
        testing_threshold_days: float = 7.0,
        consecutive_weeks_data: List[Dict] = None,
    ) -> List[FlowAlert]:
        """
        Generate health alerts based on defined rules.

        Args:
            signal: Statistical signal analysis
            trend: Trend analysis
            volatility: Volatility metrics
            flow_efficiency: Current flow efficiency
            testing_aging_p85: P85 aging for testing column
            testing_threshold_days: Threshold for testing bottleneck
            consecutive_weeks_data: Historical data for consecutive week analysis

        Returns:
            List of triggered alerts
        """
        alerts = []

        # Rule 1: Probable accumulation (CI entirely > 0 for 2 consecutive weeks)
        accumulation_alert = FlowAlert(
            id="probable_accumulation",
            title="Probable Accumulation",
            triggered=False,
            rationale="Net Flow CI entirely > 0 for consecutive periods",
            remediation="Throttle intake or increase throughput capacity",
        )

        if signal.signal_label == SignalLabel.LIKELY_ACCUMULATION.value:
            # Check if previous week was also likely accumulation (simplified check)
            accumulation_alert.triggered = True
            accumulation_alert.rationale = (
                f"Net Flow CI [{signal.ci_low:.1f}, {signal.ci_high:.1f}] indicates likely accumulation"
            )

        alerts.append(accumulation_alert)

        # Rule 2: Unstable intake (Arrival CV > 30% for multiple weeks)
        intake_alert = FlowAlert(
            id="unstable_intake",
            title="Unstable Intake",
            triggered=volatility.arrivals_cv > 30.0,
            rationale=f"Arrival volatility at {volatility.arrivals_cv:.1f}% exceeds 30% threshold",
            remediation="Implement intake planning and smoothing mechanisms",
        )
        alerts.append(intake_alert)

        # Rule 3: Testing bottleneck sustained
        testing_alert = FlowAlert(
            id="testing_bottleneck",
            title="Testing Bottleneck Sustained",
            triggered=(flow_efficiency < 40.0 and testing_aging_p85 > testing_threshold_days),
            rationale=f"Flow efficiency {flow_efficiency:.1f}% < 40% AND Testing aging P85 {testing_aging_p85:.1f} > {testing_threshold_days} days",
            remediation="Implement WIP limits and test automation",
        )
        alerts.append(testing_alert)

        # Rule 4: Intake drift (EWMA Flow Ratio > 110% for 2 weeks)
        drift_alert = FlowAlert(
            id="intake_drift",
            title="Intake Outweighs Throughput",
            triggered=(trend.current_ewma > 110.0 and trend.direction == "↑"),
            rationale=f"EWMA Flow Ratio at {trend.current_ewma:.1f}% indicates intake exceeding throughput",
            remediation="Stabilize arrival rate or increase delivery capacity",
        )
        alerts.append(drift_alert)

        return alerts

    def analyze_assignment_metrics(self, all_issues: List[Dict], project_key: str) -> AssignmentMetrics:
        """
        Analyze assignment and ownership tracking metrics.

        Args:
            all_issues: List of all issues (arrivals + completed) with full history

        Returns:
            AssignmentMetrics with ownership analysis
        """
        assignment_lags = []
        reassignment_counts = []
        team_load_assigned = defaultdict(int)
        team_load_wip = defaultdict(int)
        unassigned_count = 0
        total_issues = len(all_issues)
        handoff_scores = []

        for issue in all_issues:
            # Analyze assignment history from changelog
            assignment_history = self._extract_assignment_history(issue)

            # Calculate assignment lag (created to first assignment)
            created_str = issue.get("fields", {}).get("created", "")
            created_date = self._parse_datetime(created_str)
            if not created_date:
                continue  # Skip if no valid created date

            if assignment_history:
                first_assignment = assignment_history[0]
                lag_hours = (first_assignment["timestamp"] - created_date).total_seconds() / 3600
                assignment_lags.append(lag_hours)

                # Count reassignments (excluding None assignments)
                valid_assignments = [a for a in assignment_history if a["assignee"] is not None]
                reassignment_counts.append(len(valid_assignments) - 1 if len(valid_assignments) > 1 else 0)

                # Handoff quality (fewer reassignments = better quality)
                handoff_quality = max(0, 100 - (len(valid_assignments) - 1) * 20)
                handoff_scores.append(handoff_quality)
            else:
                # Issue was never assigned
                unassigned_count += 1
                handoff_scores.append(0)  # Poor handoff quality

            # Current assignee for load balancing
            current_assignee = issue.get("fields", {}).get("assignee")
            if current_assignee:
                assignee_name = current_assignee.get("displayName", "Unknown")
                team_load_assigned[assignee_name] += 1

                # Count WIP issues per assignee if current status is active
                status_name = issue.get("fields", {}).get("status", {}).get("name")
                if status_name:
                    active_statuses = self.workflow_service.get_active_statuses(project_key)
                    if status_name in active_statuses:
                        team_load_wip[assignee_name] += 1

        # Calculate metrics
        assignment_lag_avg = np.mean(assignment_lags) if assignment_lags else 0
        assignment_lag_p85 = np.percentile(assignment_lags, 85) if assignment_lags else 0
        unassigned_percentage = (unassigned_count / total_issues * 100) if total_issues > 0 else 0
        reassignment_frequency = np.mean(reassignment_counts) if reassignment_counts else 0
        handoff_quality_score = np.mean(handoff_scores) if handoff_scores else 0

        return AssignmentMetrics(
            assignment_lag_avg=assignment_lag_avg,
            assignment_lag_p85=assignment_lag_p85,
            unassigned_count=unassigned_count,
            unassigned_percentage=unassigned_percentage,
            reassignment_frequency=reassignment_frequency,
            team_load_balance=dict(team_load_assigned),
            team_load_wip=dict(team_load_wip),
            handoff_quality_score=handoff_quality_score,
        )

    def _extract_assignment_history(self, issue: Dict) -> List[Dict]:
        """Extract assignment history from issue changelog."""
        assignment_history = []
        changelog = issue.get("changelog", {}).get("histories", [])

        for history in changelog:
            history_time = self._parse_datetime(history.get("created"))
            if not history_time:
                continue  # Skip invalid timestamps
            for item in history["items"]:
                if item["field"] == "assignee":
                    assignment_history.append(
                        {
                            "timestamp": history_time,
                            "assignee": item.get("toString"),
                            "from_assignee": item.get("fromString"),
                        }
                    )

        return sorted(assignment_history, key=lambda x: x["timestamp"])

    def analyze_cycle_time_heatmap(
        self,
        completed_issues: List[Dict],
        project_key: str,
        teams: Optional[List[str]] = None,
    ) -> CycleTimeHeatmap:
        """
        Analyze cycle time breakdown by workflow stages.

        Args:
            completed_issues: List of completed issues with full changelog
            project_key: JIRA project key for workflow configuration

        Returns:
            CycleTimeHeatmap with stage-by-stage breakdown
        """
        # Get workflow configuration and cycle window
        workflow_config = self.workflow_service.get_workflow_config(project_key)
        workflow_stages = workflow_config.get("workflow_stages", [])
        try:
            cycle_start, cycle_end = self.workflow_service.get_cycle_time_statuses(project_key)
        except Exception:
            cycle_start = self.workflow_service.get_semantic_status(project_key, "development_start") or "07 Started"
            cycle_end = self.workflow_service.get_semantic_status(project_key, "completed") or "10 Done"

        # If no stages configured, use common defaults
        if not workflow_stages:
            workflow_stages = ["To Do", "In Progress", "Code Review", "Testing", "Done"]

        transitions = defaultdict(list)  # {(from_stage, to_stage): [transition_times]}
        stage_times = defaultdict(list)  # {stage: [time_in_stage]}

        for issue in completed_issues:
            stage_transitions = self._extract_stage_transitions(issue, workflow_stages)

            # Filter transitions to cycle window [cycle_start .. cycle_end)
            active = False
            for t in stage_transitions:
                from_stage = t["from_stage"]
                to_stage = t["to_stage"]
                time_hours = max(0.0, t["time_hours"])  # guard against negatives

                if not active:
                    if from_stage == cycle_start:
                        active = True
                    elif to_stage == cycle_start:
                        active = True
                        # do not include pre-start duration
                        continue
                    else:
                        continue

                transitions[(from_stage, to_stage)].append(time_hours)
                if from_stage != "Created":
                    stage_times[from_stage].append(time_hours)

                if to_stage == cycle_end:
                    break

        # Calculate transition metrics
        transition_metrics = []
        for (from_stage, to_stage), times in transitions.items():
            if times:  # Only include transitions that occurred
                transition_metrics.append(
                    StageTransition(
                        from_stage=from_stage,
                        to_stage=to_stage,
                        avg_time_hours=np.mean(times),
                        median_time_hours=np.median(times),
                        p85_time_hours=np.percentile(times, 85),
                        issue_count=len(times),
                    )
                )

        # Find bottleneck stage (highest P85 time)
        bottleneck_stage = "Unknown"
        max_p85_time = 0
        efficiency_by_stage = {}

        for stage, times in stage_times.items():
            if times:
                p85_time = np.percentile(times, 85)
                efficiency_by_stage[stage] = np.mean(times)

                if p85_time > max_p85_time:
                    max_p85_time = p85_time
                    bottleneck_stage = stage

        # Calculate total cycle time
        total_cycle_time = sum(np.mean(times) for times in stage_times.values())

        # Optional: per-team breakdown when multiple teams are filtered
        # Attempt to segment by squad field when provided
        per_team = []
        if teams and len(teams) > 1:
            squad_field = self.workflow_service.get_custom_field(project_key, "squad_field")
            if squad_field:
                for team in teams:
                    # Filter issues by team label in custom field
                    team_issues = []
                    for issue in completed_issues:
                        val = issue.get("fields", {}).get(squad_field)
                        # Handle string or dict value
                        if isinstance(val, dict):
                            val = val.get("value") or val.get("name") or val.get("id")
                        if isinstance(val, str) and val.strip() == team:
                            team_issues.append(issue)

                    if not team_issues:
                        continue

                    # Recompute stage times for the team subset (cycle window only)
                    team_stage_times = defaultdict(list)
                    team_transitions = []
                    for issue in team_issues:
                        all_t = self._extract_stage_transitions(issue, workflow_stages)
                        active = False
                        for t in all_t:
                            from_stage = t["from_stage"]
                            to_stage = t["to_stage"]
                            time_hours = max(0.0, t["time_hours"])  # guard against negatives
                            if not active:
                                if from_stage == cycle_start:
                                    active = True
                                elif to_stage == cycle_start:
                                    active = True
                                    continue
                                else:
                                    continue
                            team_transitions.append(t)
                            if from_stage != "Created":
                                team_stage_times[from_stage].append(time_hours)
                            if to_stage == cycle_end:
                                break

                    team_bottleneck = "Unknown"
                    team_max_p85 = 0
                    for stage, times in team_stage_times.items():
                        if times:
                            p85_time = np.percentile(times, 85)
                            if p85_time > team_max_p85:
                                team_max_p85 = p85_time
                                team_bottleneck = stage

                    team_total_cycle = sum(np.mean(times) for times in team_stage_times.values())
                    per_team.append(
                        {
                            "team": team,
                            "issue_count": len(team_issues),
                            "total_cycle_time_days": team_total_cycle / 24.0,
                            "bottleneck_stage": team_bottleneck or "Unknown",
                        }
                    )

        return CycleTimeHeatmap(
            transitions=transition_metrics,
            total_cycle_time=total_cycle_time,
            bottleneck_stage=bottleneck_stage,
            efficiency_by_stage=efficiency_by_stage,
        )

    def _extract_stage_transitions(self, issue: Dict, workflow_stages: List[str]) -> List[Dict]:
        """Extract stage transitions from issue changelog (chronologically, non-negative durations)."""
        transitions = []
        histories = issue.get("changelog", {}).get("histories", [])

        # Start with creation
        created_date = self._parse_datetime(issue.get("fields", {}).get("created"))
        if not created_date:
            return []  # Skip if no creation date

        # Parse and sort histories chronologically
        parsed_histories = []
        for h in histories:
            ts = h.get("created")
            if not ts:
                continue
            dt = self._parse_datetime(ts)
            if not dt:
                continue
            parsed_histories.append((dt, h))
        parsed_histories.sort(key=lambda x: x[0])

        current_stage = "Created"
        last_transition_time = created_date

        for history_time, history in parsed_histories:
            # Only consider status changes
            for item in history.get("items", []):
                if item.get("field") == "status":
                    to_stage = item.get("toString", "Unknown")
                    # Calculate time in previous stage (clamp negatives)
                    time_in_stage = (history_time - last_transition_time).total_seconds() / 3600
                    if time_in_stage < 0:
                        time_in_stage = 0.0

                    transitions.append(
                        {
                            "from_stage": current_stage,
                            "to_stage": to_stage,
                            "time_hours": time_in_stage,
                            "timestamp": history_time,
                        }
                    )

                    current_stage = to_stage
                    last_transition_time = history_time

        return transitions

    def generate_net_flow_scorecard(
        self,
        project_key: str,
        end_date: str,
        issue_types: Optional[List[str]] = None,
        teams: Optional[List[str]] = None,
        include_subtasks: bool = False,
        output_format: str = "console",
        verbose: bool = False,
        # New statistical parameters
        enable_statistical_analysis: bool = False,
        enable_ci: bool = False,
        enable_ewma: bool = False,
        enable_cusum: bool = False,
        cv_window: int = 8,
        alpha: float = 0.2,
        active_devs: Optional[int] = None,
        testing_threshold_days: float = 7.0,
    ) -> Dict:
        """
        Generates a net flow scorecard with a rolling 4-week trend.
        """
        try:
            self.logger.info(f"Generating Net Flow Scorecard for project {project_key} with anchor date {end_date}")

            done_statuses = self.workflow_service.get_done_statuses(project_key)

            # Anchor analysis on the provided end_date and look BACKWARD.
            # Keep the same anchoring behavior as other commands (no forward shift).
            anchor_dt = self._parse_datetime(end_date)
            if not anchor_dt:
                raise ValueError(f"Invalid end date format: {end_date}")
            primary_end = anchor_dt
            primary_start = primary_end - timedelta(days=6)

            rolling_trend_metrics = []
            all_completed_issues = []  # raw completed for current period
            all_arrival_issues = []  # raw arrivals for current period
            for i in range(4):
                start_dt = primary_start - timedelta(days=7 * i)
                end_dt = primary_end - timedelta(days=7 * i)

                metrics, arrival_issues_raw, completed_issues_raw = self._calculate_metrics_for_period(
                    project_key,
                    start_dt,
                    end_dt,
                    issue_types,
                    teams,
                    include_subtasks,
                    done_statuses,
                    expand_for_detailed=(i == 0),
                )
                rolling_trend_metrics.append(metrics)
                if i == 0:
                    all_completed_issues = completed_issues_raw
                    all_arrival_issues = arrival_issues_raw

            rolling_trend_metrics.reverse()

            current_week_metrics = rolling_trend_metrics[-1]

            # Advanced Metrics Calculation
            flow_efficiency, bottleneck = self._analyze_flow_efficiency_and_bottlenecks(
                all_completed_issues, project_key
            )
            arrival_rates = [m["arrival_rate"] for m in rolling_trend_metrics]
            arrival_volatility = self._calculate_volatility(arrival_rates)

            flow_status = self._determine_flow_status(
                current_week_metrics["net_flow"],
                current_week_metrics["arrival_rate"],
                current_week_metrics["throughput"],
            )
            flow_ratio = (
                (current_week_metrics["throughput"] / current_week_metrics["arrival_rate"] * 100)
                if current_week_metrics["arrival_rate"] > 0
                else 0
            )

            # Initialize optional statistical analyses
            statistical_signal = None
            trend_analysis = None
            volatility_metrics = None
            flow_debt = 0
            normalized_metrics = {}
            segments = []
            alerts = []
            assignment_metrics = None
            cycle_time_heatmap = None

            # Statistical Analysis (enabled by flags or enable_statistical_analysis)
            if enable_statistical_analysis or enable_ci or enable_ewma:
                # Bootstrap confidence interval for current week net flow
                if enable_statistical_analysis or enable_ci:
                    statistical_signal = self.bootstrap_net_flow_ci(
                        current_week_metrics["arrival_rate"],
                        current_week_metrics["throughput"],
                    )

                # EWMA trend analysis
                if enable_statistical_analysis or enable_ewma:
                    flow_ratios = [
                        (m["throughput"] / m["arrival_rate"] * 100) if m["arrival_rate"] > 0 else 0
                        for m in rolling_trend_metrics
                    ]
                    trend_analysis = self.compute_ewma(flow_ratios, alpha)

                    # CUSUM shift detection
                    if enable_cusum:
                        cusum_shift = self.detect_cusum_shift(flow_ratios)
                        if trend_analysis:
                            trend_analysis.cusum_shift_detected = cusum_shift

                # Volatility and stability metrics
                if enable_statistical_analysis:
                    throughput_rates = [m["throughput"] for m in rolling_trend_metrics]
                    net_flows = [m["net_flow"] for m in rolling_trend_metrics]

                    arrivals_cv = self.compute_rolling_cv(arrival_rates, cv_window)[-1] or 0.0
                    throughput_cv = self.compute_rolling_cv(throughput_rates, cv_window)[-1] or 0.0
                    stability_index = self.compute_stability_index(net_flows, cv_window)

                    volatility_metrics = VolatilityMetrics(arrivals_cv, throughput_cv, stability_index)

                    # Flow debt calculation
                    flow_debt = self.compute_flow_debt(net_flows)

                    # Normalized metrics
                    if active_devs:
                        normalized_metrics = {
                            "net_flow_per_dev": self.normalize_per_dev(current_week_metrics["net_flow"], active_devs),
                            "arrival_rate_per_dev": self.normalize_per_dev(
                                current_week_metrics["arrival_rate"], active_devs
                            ),
                            "throughput_per_dev": self.normalize_per_dev(
                                current_week_metrics["throughput"], active_devs
                            ),
                            "active_devs": active_devs,
                        }

                    # Segmentation analysis
                    # Build segments based on summaries from current period
                    current_arrival_summaries = [self._extract_issue_summary(i) for i in all_arrival_issues]
                    current_completed_summaries = [self._extract_issue_summary(i) for i in all_completed_issues]
                    segments = self.analyze_segments_by_type(current_arrival_summaries, current_completed_summaries)

                    # Health alerts
                    if statistical_signal and trend_analysis and volatility_metrics:
                        alerts = self.generate_health_alerts(
                            statistical_signal,
                            trend_analysis,
                            volatility_metrics,
                            flow_efficiency,
                            testing_threshold_days=testing_threshold_days,
                        )

            # Assignment & Ownership Analysis using RAW issues from current period
            all_issues_raw = list(all_arrival_issues) + list(all_completed_issues)
            if all_issues_raw:
                assignment_metrics = self.analyze_assignment_metrics(all_issues_raw, project_key)

                # Cycle Time Heatmap Analysis
            if all_completed_issues:
                cycle_time_heatmap = self.analyze_cycle_time_heatmap(all_completed_issues, project_key, teams=teams)

            # Prepare team label similar to IssueAdherenceService
            team_label = None
            if teams:
                seen = set()
                ordered = []
                for t in teams:
                    if t and t not in seen:
                        seen.add(t)
                        ordered.append(t)
                team_label = ", ".join(ordered) if ordered else None

            week_start_dt = self._parse_datetime(current_week_metrics["start_date"])
            if not week_start_dt:
                raise ValueError("Invalid start date in current week metrics")

            rolling_trend_payload = []
            for m in rolling_trend_metrics:
                start_dt = self._parse_datetime(m["start_date"])
                if not start_dt:
                    continue
                rolling_trend_payload.append(
                    {
                        "week_number": start_dt.isocalendar()[1],
                        "net_flow": m["net_flow"],
                        "start_date": m["start_date"],
                        "end_date": m["end_date"],
                        "arrival_rate": m["arrival_rate"],
                        "throughput": m["throughput"],
                    }
                )

            response = {
                "metadata": {
                    "project_key": project_key,
                    "anchor_date": end_date,
                    "team": team_label,
                    "teams": teams,
                    "issue_types": issue_types,
                    "include_subtasks": include_subtasks,
                    "analysis_date": datetime.now().isoformat(),
                    "week_number": week_start_dt.isocalendar()[1],
                    "start_date": current_week_metrics["start_date"],
                    "end_date": current_week_metrics["end_date"],
                    "statistical_analysis_enabled": bool(enable_statistical_analysis),
                },
                "current_week": {
                    "arrival_rate": current_week_metrics["arrival_rate"],
                    "throughput": current_week_metrics["throughput"],
                    "net_flow": current_week_metrics["net_flow"],
                    "flow_ratio": flow_ratio,
                    "flow_status": flow_status,
                    "flow_efficiency": flow_efficiency,
                    "primary_bottleneck": bottleneck,
                    "arrival_volatility": arrival_volatility,
                },
                "rolling_4_weeks_trend": rolling_trend_payload,
                "insights": self._generate_insights(
                    current_week_metrics["net_flow"],
                    current_week_metrics["arrival_rate"],
                    current_week_metrics["throughput"],
                ),
                # Only include detailed issues list when verbose is requested
                "details": {
                    "arrival_issues": [self._extract_issue_summary(i) for i in all_arrival_issues],
                    "completed_issues": [self._extract_issue_summary(i) for i in all_completed_issues],
                }
                if verbose
                else {},
            }

            # Add statistical analysis results to response
            if statistical_signal:
                response["statistical_signal"] = {
                    "net_flow_ci_low": statistical_signal.ci_low,
                    "net_flow_ci_high": statistical_signal.ci_high,
                    "signal_label": statistical_signal.signal_label,
                    "confidence_level": statistical_signal.confidence_level,
                }

            if trend_analysis:
                response["trend_analysis"] = {
                    "ewma_flow_ratio": trend_analysis.current_ewma,
                    "trend_direction": trend_analysis.direction,
                    "cusum_shift_detected": bool(trend_analysis.cusum_shift_detected),
                }

            if volatility_metrics:
                response["volatility_metrics"] = {
                    "arrivals_cv": volatility_metrics.arrivals_cv,
                    "throughput_cv": volatility_metrics.throughput_cv,
                    "stability_index": volatility_metrics.stability_index,
                }

            if flow_debt > 0:
                response["flow_debt"] = flow_debt

            if normalized_metrics:
                response["normalized_metrics"] = normalized_metrics

            if segments:
                response["segments"] = [
                    {
                        "segment_name": seg.segment_name,
                        "arrivals": seg.arrivals,
                        "throughput": seg.throughput,
                        "net_flow": seg.net_flow,
                    }
                    for seg in segments
                ]

            if alerts:
                response["alerts"] = [
                    {
                        "id": alert.id,
                        "title": alert.title,
                        "triggered": bool(alert.triggered),
                        "rationale": alert.rationale,
                        "remediation": alert.remediation,
                    }
                    for alert in alerts
                ]

            # Add Assignment & Ownership Metrics
            if assignment_metrics:
                response["assignment_metrics"] = {
                    "assignment_lag_avg_hours": assignment_metrics.assignment_lag_avg,
                    "assignment_lag_p85_hours": assignment_metrics.assignment_lag_p85,
                    "unassigned_count": assignment_metrics.unassigned_count,
                    "unassigned_percentage": assignment_metrics.unassigned_percentage,
                    "reassignment_frequency": assignment_metrics.reassignment_frequency,
                    "team_load_balance": assignment_metrics.team_load_balance,
                    "team_load_wip": assignment_metrics.team_load_wip,
                    "handoff_quality_score": assignment_metrics.handoff_quality_score,
                }

            # Add Cycle Time Heatmap
            if cycle_time_heatmap:
                response["cycle_time_heatmap"] = {
                    "transitions": [
                        {
                            "from_stage": t.from_stage,
                            "to_stage": t.to_stage,
                            "avg_time_hours": t.avg_time_hours,
                            "avg_time_days": t.avg_time_hours / 24,
                            "median_time_hours": t.median_time_hours,
                            "p85_time_hours": t.p85_time_hours,
                            "issue_count": t.issue_count,
                        }
                        for t in cycle_time_heatmap.transitions
                    ],
                    "total_cycle_time_hours": cycle_time_heatmap.total_cycle_time,
                    "total_cycle_time_days": cycle_time_heatmap.total_cycle_time / 24,
                    "bottleneck_stage": cycle_time_heatmap.bottleneck_stage,
                    "efficiency_by_stage": cycle_time_heatmap.efficiency_by_stage,
                }

                # If multiple teams are filtered, include a team-level summary to improve readability
                if teams and isinstance(teams, list) and len(teams) > 1 and all_completed_issues:
                    squad_field = self.workflow_service.get_custom_field(project_key, "squad_field")
                    wf_stages = self.workflow_service.get_workflow_config(project_key).get("workflow_stages", [])
                    by_team = []
                    if squad_field:
                        for team_name in teams:
                            subset = []
                            for iss in all_completed_issues:
                                val = iss.get("fields", {}).get(squad_field)
                                if isinstance(val, dict):
                                    val = val.get("value") or val.get("name") or val.get("id")
                                if isinstance(val, str) and val.strip() == team_name:
                                    subset.append(iss)
                            if not subset:
                                continue

                            # Compute quick bottleneck and total cycle time for the team
                            team_stage_times = defaultdict(list)
                            for iss in subset:
                                for t in self._extract_stage_transitions(iss, wf_stages):
                                    if t["from_stage"] != "Created":
                                        team_stage_times[t["from_stage"]].append(t["time_hours"])

                            team_bottleneck = "Unknown"
                            max_p85 = 0
                            for stage, times in team_stage_times.items():
                                if times:
                                    p85 = np.percentile(times, 85)
                                    if p85 > max_p85:
                                        max_p85 = p85
                                        team_bottleneck = stage

                            total_cycle = sum(np.mean(times) for times in team_stage_times.values())
                            by_team.append(
                                {
                                    "team": team_name,
                                    "issue_count": len(subset),
                                    "total_cycle_time_days": total_cycle / 24.0,
                                    "bottleneck_stage": team_bottleneck,
                                }
                            )

                    if by_team:
                        response["cycle_time_heatmap"]["by_team"] = by_team

            # Save reports into a per-day folder (all reports for the same day go together)
            date_str = datetime.now().strftime("%Y%m%d")
            sub_dir = f"net-flow_{date_str}"

            saved_path = None
            if output_format == "json":
                saved_path = OutputManager.save_json_report(
                    response,
                    sub_dir,
                    f"net_flow_scorecard_{project_key}",
                )
            elif output_format == "md":
                markdown_content = self._format_as_markdown(response)
                saved_path = OutputManager.save_markdown_report(
                    markdown_content,
                    sub_dir,
                    f"net_flow_scorecard_{project_key}",
                )

            if saved_path:
                response["output_file"] = saved_path

            self.logger.info("Net Flow Scorecard generation completed successfully.")
            return response

        except Exception as e:
            self.logger.error(f"Error in scorecard generation: {e}", exc_info=True)
            raise

    def _calculate_metrics_for_period(
        self,
        project_key: str,
        start_date: datetime,
        end_date: datetime,
        issue_types: List[str],
        teams: Optional[List[str]],
        include_subtasks: bool,
        done_statuses: List[str],
        expand_for_detailed: bool = False,
    ) -> tuple[Dict, List, List]:
        """Calculates arrival, throughput, and net flow for a single time period.

        Returns a tuple of (metrics, arrival_issues_raw, completed_issues_raw).
        """
        arrival_issues = self._fetch_created_issues(
            project_key,
            start_date,
            end_date,
            issue_types,
            teams,
            include_subtasks,
            expand_changelog=expand_for_detailed,
        )
        completed_issues = self._fetch_completed_issues(
            project_key,
            start_date,
            end_date,
            issue_types,
            teams,
            include_subtasks,
            done_statuses,
            expand_changelog=expand_for_detailed,
        )

        metrics = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "arrival_rate": len(arrival_issues),
            "throughput": len(completed_issues),
            "net_flow": len(arrival_issues) - len(completed_issues),
        }

        metrics["arrival_issues"] = [self._extract_issue_summary(i) for i in arrival_issues]
        metrics["completed_issues"] = [self._extract_issue_summary(i) for i in completed_issues]

        return metrics, arrival_issues, completed_issues

    def _analyze_flow_efficiency_and_bottlenecks(
        self, completed_issues: List[Dict], project_key: str
    ) -> tuple[float, str]:
        # TODO: Move status configuration to WorkflowConfigService
        active_statuses = self.workflow_service.get_active_statuses(project_key)
        waiting_statuses = self.workflow_service.get_waiting_statuses(project_key)

        total_work_time_seconds = 0
        total_cycle_time_seconds = 0
        status_time_seconds = defaultdict(float)

        for issue in completed_issues:
            changelog = issue.get("changelog", {}).get("histories", [])
            if not changelog:
                continue

            issue_work_time = 0
            issue_cycle_time = 0
            first_active_time = None
            last_status_change_time = self._parse_datetime(issue.get("fields", {}).get("created"))
            if not last_status_change_time:
                continue

            for history in changelog:
                history_time = self._parse_datetime(history.get("created"))
                if not history_time:
                    continue
                for item in history["items"]:
                    if item["field"] == "status":
                        from_status = item["fromString"]

                        time_in_from_status = (history_time - last_status_change_time).total_seconds()

                        if from_status in active_statuses or from_status in waiting_statuses:
                            if first_active_time is None and from_status in active_statuses:
                                first_active_time = last_status_change_time

                            if first_active_time:
                                issue_cycle_time += time_in_from_status
                                status_time_seconds[from_status] += time_in_from_status
                                if from_status in active_statuses:
                                    issue_work_time += time_in_from_status

                        last_status_change_time = history_time

            total_work_time_seconds += issue_work_time
            total_cycle_time_seconds += issue_cycle_time

        flow_efficiency = (
            (total_work_time_seconds / total_cycle_time_seconds * 100) if total_cycle_time_seconds > 0 else 0
        )
        # Clamp to [0, 100] to avoid nonsensical values due to partial changelogs or edge cases
        if flow_efficiency < 0:
            flow_efficiency = 0.0
        if flow_efficiency > 100:
            flow_efficiency = 100.0

        bottleneck = (
            max(status_time_seconds.keys(), key=lambda k: status_time_seconds[k]) if status_time_seconds else "N/A"
        )

        return flow_efficiency, bottleneck

    def _calculate_volatility(self, data: List[float]) -> float:
        if not data or len(data) < 2:
            return 0.0

        mean = sum(data) / len(data)
        if mean == 0:
            return 0.0

        variance = sum([(x - mean) ** 2 for x in data]) / len(data)
        std_dev = math.sqrt(variance)

        return (std_dev / mean) * 100

    def _fetch_created_issues(
        self,
        project_key: str,
        start_date: datetime,
        end_date: datetime,
        issue_types: list,
        teams: Optional[List[str]],
        include_subtasks: bool,
        expand_changelog: bool = False,
    ) -> list:
        """Fetch issues created in the specified time period."""
        jql_parts = [f"project = '{project_key}'"]
        if issue_types:
            types_str = "', '".join(issue_types)
            jql_parts.append(f"type in ('{types_str}')")
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        jql_parts.append(f"created >= '{start_date_str}' AND created <= '{end_date_str}'")
        # Team filter (supports one or many teams)
        if teams:
            cleaned = []
            seen = set()
            for t in teams:
                if t:
                    t = t.strip()
                    if t and t not in seen:
                        seen.add(t)
                        cleaned.append(t)
            if cleaned:
                squad_field = self.workflow_service.get_custom_field(project_key, "squad_field")
                if squad_field:
                    if len(cleaned) == 1:
                        jql_parts.append(f"'{squad_field}' = '{cleaned[0]}'")
                    else:
                        vals = "', '".join(cleaned)
                        jql_parts.append(f"'{squad_field}' in ('{vals}')")
        if not include_subtasks:
            jql_parts.append("type != Sub-task")
        jql_query = " AND ".join(jql_parts)
        self.logger.info(f"Fetching created issues with JQL: {jql_query}")
        # Ensure we include the configured squad field for team segmentation
        squad_field = self.workflow_service.get_custom_field(project_key, "squad_field")
        base_fields = [
            "key",
            "summary",
            "issuetype",
            "status",
            "created",
            "assignee",
            "customfield_10265",
        ]
        if squad_field and squad_field not in base_fields:
            base_fields.append(squad_field)
        fields_str = ",".join(base_fields)

        issues = self.jira_assistant.fetch_issues(
            jql_query=jql_query,
            fields=fields_str,
            max_results=1000,
            expand_changelog=expand_changelog,
        )
        self.logger.info(f"Found {len(issues)} issues created in period")
        return issues

    def _fetch_completed_issues(
        self,
        project_key: str,
        start_date: datetime,
        end_date: datetime,
        issue_types: list,
        teams: Optional[List[str]],
        include_subtasks: bool,
        done_statuses: list,
        expand_changelog: bool = False,
    ) -> list:
        """Fetch issues completed in the specified time period."""
        jql_parts = [f"project = '{project_key}'"]
        if issue_types:
            types_str = "', '".join(issue_types)
            jql_parts.append(f"type in ('{types_str}')")
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        jql_parts.append(f"resolved >= '{start_date_str}' AND resolved <= '{end_date_str}'")
        if done_statuses:
            status_str = "', '".join(done_statuses)
            jql_parts.append(f"status in ('{status_str}')")
        # Team filter (supports one or many teams)
        if teams:
            cleaned = []
            seen = set()
            for t in teams:
                if t:
                    t = t.strip()
                    if t and t not in seen:
                        seen.add(t)
                        cleaned.append(t)
            if cleaned:
                squad_field = self.workflow_service.get_custom_field(project_key, "squad_field")
                if squad_field:
                    if len(cleaned) == 1:
                        jql_parts.append(f"'{squad_field}' = '{cleaned[0]}'")
                    else:
                        vals = "', '".join(cleaned)
                        jql_parts.append(f"'{squad_field}' in ('{vals}')")
        if not include_subtasks:
            jql_parts.append("type != Sub-task")
        jql_query = " AND ".join(jql_parts)
        self.logger.info(f"Fetching completed issues with JQL: {jql_query}")
        # Ensure we include the configured squad field for team segmentation
        squad_field = self.workflow_service.get_custom_field(project_key, "squad_field")
        base_fields = [
            "key",
            "summary",
            "issuetype",
            "status",
            "created",
            "resolved",
            "assignee",
            "customfield_10265",
        ]
        if squad_field and squad_field not in base_fields:
            base_fields.append(squad_field)
        fields_str = ",".join(base_fields)

        issues = self.jira_assistant.fetch_issues(
            jql_query=jql_query,
            fields=fields_str,
            max_results=1000,
            expand_changelog=expand_changelog,
        )
        self.logger.info(f"Found {len(issues)} issues completed in period")
        return issues

    def _extract_issue_summary(self, issue: dict) -> dict:
        """Extract summary information from an issue."""
        fields = issue.get("fields", {})
        return {
            "key": issue.get("key"),
            "summary": fields.get("summary"),
            "type": fields.get("issuetype", {}).get("name"),
            "status": fields.get("status", {}).get("name"),
            "assignee": fields.get("assignee", {}).get("displayName") if fields.get("assignee") else None,
            "created": fields.get("created"),
            "resolved": fields.get("resolved"),
        }

    def _determine_flow_status(self, net_flow: int, arrival_rate: int, throughput: int) -> str:
        """Determine flow status based on net flow value."""
        if net_flow > 0:
            if net_flow > (arrival_rate * 0.2):  # More than 20% imbalance
                return "CRITICAL_BOTTLENECK"
            else:
                return "MINOR_BOTTLENECK"
        elif net_flow < 0:
            return "HEALTHY_FLOW"
        else:
            return "BALANCED"

    def _generate_insights(self, net_flow: int, arrival_rate: int, throughput: int) -> list:
        """Generate insights based on flow metrics."""
        insights = []
        if net_flow > 0:
            insights.append(
                f"⚠️  Work is arriving faster than being completed ({net_flow} more arrivals than completions)"
            )
            insights.append("💡 Consider: increasing team capacity, reducing scope, or improving process efficiency")
            if arrival_rate > 0:
                backlog_growth_rate = (net_flow / arrival_rate) * 100
                insights.append(f"📈 Backlog growing at {backlog_growth_rate:.1f}% rate relative to arrival rate")
        elif net_flow < 0:
            insights.append(f"✅ Healthy flow: completing more work than arriving ({abs(net_flow)} more completions)")
            insights.append("💡 Consider: taking on additional work or focusing on higher-value items")
        else:
            insights.append("⚖️  Perfectly balanced: arrival rate equals throughput")
        if arrival_rate > 0:
            efficiency = (throughput / arrival_rate) * 100
            if efficiency < 50:
                insights.append(f"🐌 Low throughput efficiency: {efficiency:.1f}% - investigate bottlenecks")
            elif efficiency > 120:
                insights.append(f"🚀 High throughput efficiency: {efficiency:.1f}% - sustainable pace?")
        if arrival_rate == 0 and throughput == 0:
            insights.append("🔍 No activity detected in this period - check filters or time range")
        elif arrival_rate == 0:
            insights.append("📉 No new work arriving - focus on completing existing backlog")
        elif throughput == 0:
            insights.append("🚫 No work being completed - investigate delivery blockers")
        return insights

    def _save_results(self, results: dict, output_file: str):
        """Save results to JSON file."""
        try:
            JSONManager.write_json(results, output_file)
            self.logger.info(f"Net flow results saved to {output_file}")
        except Exception as e:
            self.logger.error(f"Failed to save results to {output_file}: {e}")
            raise

    def _format_as_markdown(self, scorecard: Dict) -> str:
        """
        Formats the net flow scorecard as structured markdown following PyToolkit patterns.

        Args:
            scorecard (Dict): The complete scorecard data structure

        Returns:
            str: Markdown formatted content following PyToolkit style
        """
        metadata = scorecard.get("metadata", {})
        current_week = scorecard.get("current_week", {})
        trend = scorecard.get("rolling_4_weeks_trend", [])
        details = scorecard.get("details", {})

        # Extract fields
        project_key = metadata.get("project_key", "N/A")
        week_number = metadata.get("week_number", "N/A")
        start_date = metadata.get("start_date", "")
        end_date = metadata.get("end_date", "")
        team = metadata.get("team", "All Teams")
        analysis_date = metadata.get("analysis_date", "")
        statistical_enabled = metadata.get("statistical_analysis_enabled", False)

        # Determine status
        net_flow = current_week.get("net_flow", 0)
        if net_flow > 5:
            status_emoji = "🔴"
            status_text = "Critical Risk - Severe backlog accumulation"
        elif net_flow > 0:
            status_emoji = "🟡"
            status_text = "At Risk - Backlog growing"
        elif net_flow == 0:
            status_emoji = "🟢"
            status_text = "Balanced - Optimal flow"
        else:
            status_emoji = "🟢"
            status_text = "Healthy - Backlog reducing"

        markdown_content = f"""# {status_emoji} JIRA Net Flow Health Report

**Project:** {project_key}
**Analysis Period:** Week {week_number}
**Date Range:** {start_date} to {end_date}
**Team Filter:** {team}
**Generated:** {analysis_date}

## 📊 Executive Summary

**Net Flow:** {net_flow:+d} issues
**Status:** {status_text}
**Flow Ratio:** {current_week.get("flow_ratio", 0):.1f}%

- **Total Arrivals:** {current_week.get("arrival_rate", 0)}
- **Total Throughput:** {current_week.get("throughput", 0)}
- **Flow Efficiency:** {current_week.get("flow_efficiency", 0):.1f}%

> Concepts
> - Net Flow = Arrivals − Throughput. Positive means backlog likely grows; negative means it shrinks.
> - Flow Ratio = Throughput / Arrivals. Higher is generally healthier (>= ~85%).

"""

        # Statistical Signal Section
        if "statistical_signal" in scorecard:
            signal = scorecard["statistical_signal"]
            markdown_content += f"""## 📐 Statistical Signal Analysis

- Bootstrap 95% CI: [{signal["net_flow_ci_low"]:.1f}, {signal["net_flow_ci_high"]:.1f}]
- Signal Assessment: **{signal["signal_label"]}**
- Confidence Level: {signal["confidence_level"] * 100:.0f}%

> What does this mean?
> - Bootstrap CI: we resample arrivals/throughput (Poisson) many times to estimate a confidence interval for Net Flow.
> - If the entire CI is above 0 → likely accumulation. Entirely below 0 → likely reduction. Overlaps 0 → inconclusive/noise.

"""

        # Trend Analysis Section
        if "trend_analysis" in scorecard:
            trend_data = scorecard["trend_analysis"]
            markdown_content += f"""## 📈 Trend Analysis

- EWMA Flow Ratio: {trend_data["ewma_flow_ratio"]:.1f}% {trend_data["trend_direction"]}
- Trend Direction: {trend_data["trend_direction"]}
- CUSUM Shift Detected: {"Yes" if trend_data.get("cusum_shift_detected", False) else "No"}

> What does this mean?
> - EWMA (Exponentially Weighted Moving Average) highlights recent changes in Flow Ratio while smoothing noise.
> - CUSUM detects sustained shifts (step-changes) rather than single outliers.

"""

        # Volatility Metrics
        if "volatility_metrics" in scorecard:
            vol_metrics = scorecard["volatility_metrics"]
            markdown_content += f"""## 📊 Volatility & Stability Metrics

| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| Arrival Volatility (CV) | {vol_metrics["arrivals_cv"]:.1f}% | <30% | {"✅" if vol_metrics["arrivals_cv"] < 30 else "🔴"} |
| Throughput Volatility (CV) | {vol_metrics["throughput_cv"]:.1f}% | <30% | {"✅" if vol_metrics["throughput_cv"] < 30 else "🔴"} |
| Stability Index | {vol_metrics["stability_index"]:.1f}% | >70% | {"✅" if vol_metrics["stability_index"] > 70 else "🔴"} |

> Notes
> - Coefficient of Variation (CV) = std/mean; lower CV → more predictable flow.
> - Stability Index estimates the share of weeks operating within expected variance (higher is better).

"""

        # Flow Debt
        if "flow_debt" in scorecard:
            markdown_content += f"""## 💳 Flow Debt Analysis

**Quarter-to-Date Flow Debt:** {scorecard["flow_debt"]} issues

Flow debt represents the cumulative positive net flow over time, indicating backlog pressure buildup.

"""

        # Rolling Trend Table
        markdown_content += """## 📈 Rolling 4-Week Trend

| Week | Net Flow | Arrivals | Throughput | Status |
|------|----------|----------|------------|--------|
"""

        for i, week_data in enumerate(trend):
            week_num = week_data["week_number"]
            net_flow_week = week_data["net_flow"]
            arrival = week_data["arrival_rate"]
            throughput = week_data["throughput"]

            # Status emoji for the week
            week_status = "🔴" if net_flow_week > 5 else "🟡" if net_flow_week > 0 else "✅"

            is_current = i == len(trend) - 1
            week_marker = "**" if is_current else ""

            markdown_content += f"| {week_marker}Week {week_num}{week_marker} | {net_flow_week:+d} | {arrival} | {throughput} | {week_status} |\n"

        # Segmentation Analysis
        if "segments" in scorecard and scorecard["segments"]:
            markdown_content += """

## 👥 Segmentation Analysis

### By Issue Type

| Issue Type | Net Flow | Arrivals | Throughput | Status |
|------------|----------|----------|------------|--------|
"""

            for segment in scorecard["segments"]:
                seg_net_flow = segment["net_flow"]
                seg_status = "🔴" if seg_net_flow > 2 else "🟡" if seg_net_flow > 0 else "✅"
                markdown_content += f"| {segment['segment_name']} | {seg_net_flow:+d} | {segment['arrivals']} | {segment['throughput']} | {seg_status} |\n"

        # Health Alerts
        if "alerts" in scorecard and scorecard["alerts"]:
            triggered_alerts = [a for a in scorecard["alerts"] if a["triggered"]]
            if triggered_alerts:
                markdown_content += f"""

## 🚨 Health Alerts ({len(triggered_alerts)} triggered)
"""

                for alert in triggered_alerts:
                    markdown_content += f"""
### {alert["title"]}

**Rationale:** {alert["rationale"]}
**Remediation:** {alert["remediation"]}

"""

        # Assignment & Ownership Analysis
        if "assignment_metrics" in scorecard:
            assignment_data = scorecard["assignment_metrics"]
            markdown_content += f"""

## 👥 Assignment & Ownership Analysis

### Assignment Performance
| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| Assignment Lag (Avg) | {assignment_data["assignment_lag_avg_hours"]:.1f}h ({assignment_data["assignment_lag_avg_hours"] / 24:.1f}d) | <24h | {"✅" if assignment_data["assignment_lag_avg_hours"] < 24 else "🔴"} |
| Assignment Lag (P85) | {assignment_data["assignment_lag_p85_hours"]:.1f}h ({assignment_data["assignment_lag_p85_hours"] / 24:.1f}d) | <48h | {"✅" if assignment_data["assignment_lag_p85_hours"] < 48 else "🔴"} |
| Unassigned Issues | {assignment_data["unassigned_count"]} ({assignment_data["unassigned_percentage"]:.1f}%) | <10% | {"✅" if assignment_data["unassigned_percentage"] < 10 else "🔴"} |
| Reassignment Frequency | {assignment_data["reassignment_frequency"]:.1f} per issue | <0.5 | {"✅" if assignment_data["reassignment_frequency"] < 0.5 else "🔴"} |
| Handoff Quality Score | {assignment_data["handoff_quality_score"]:.1f}% | >80% | {"✅" if assignment_data["handoff_quality_score"] > 80 else "🔴"} |

### Team Load Distribution
"""

            if assignment_data["team_load_balance"]:
                markdown_content += "| Team Member | Assigned Issues | WIP Issues | Load Status |\n"
                markdown_content += "|--------------|-----------------|-----------|-------------|\n"

                total_assigned = sum(assignment_data["team_load_balance"].values())
                count_members = max(1, len(assignment_data["team_load_balance"]))
                avg_assigned = total_assigned / count_members
                team_wip = assignment_data.get("team_load_wip", {}) or {}

                for member, assigned in sorted(assignment_data["team_load_balance"].items()):
                    wip_count = team_wip.get(member, 0)
                    load_status = "⚖️ Balanced"
                    if assigned > avg_assigned * 1.5:
                        load_status = "🔴 Overloaded"
                    elif assigned < avg_assigned * 0.5:
                        load_status = "🟡 Underutilized"

                    markdown_content += f"| {member} | {assigned} | {wip_count} | {load_status} |\n"

        # Cycle Time Heatmap
        if "cycle_time_heatmap" in scorecard:
            heatmap_data = scorecard["cycle_time_heatmap"]
            markdown_content += f"""

## ⏱️ Cycle Time Heatmap

**Total Cycle Time:** {heatmap_data["total_cycle_time_days"]:.1f} days
**Primary Bottleneck:** {heatmap_data["bottleneck_stage"]}

### Stage Transition Analysis
"""

            if heatmap_data["transitions"]:
                markdown_content += "| Transition | Avg Time | P85 Time | Issues | Status |\n"
                markdown_content += "|------------|----------|----------|--------|--------|\n"

                # Sort transitions by average time (descending) and filter trivial (<0.1d)
                transitions = sorted(
                    [t for t in heatmap_data["transitions"] if t.get("avg_time_days", 0) >= 0.1],
                    key=lambda x: x["avg_time_hours"],
                    reverse=True,
                )

                for transition in transitions[:10]:  # Show top 10 slowest transitions
                    avg_days = transition["avg_time_days"]
                    p85_hours = transition["p85_time_hours"]

                    # Status based on time thresholds
                    status = "✅ Fast"
                    if avg_days > 3:
                        status = "🔴 Slow"
                    elif avg_days > 1:
                        status = "🟡 Moderate"

                    markdown_content += f"| {transition['from_stage']} → {transition['to_stage']} | {avg_days:.1f}d | {p85_hours:.1f}h | {transition['issue_count']} | {status} |\n"

            # Team breakdown when multiple teams are in scope
            if heatmap_data.get("by_team"):
                markdown_content += """

### By Team (Cycle Summary)

| Team | Issues | Total Cycle | Bottleneck |
|------|--------|-------------|------------|
"""

                for row in heatmap_data["by_team"]:
                    markdown_content += f"| {row['team']} | {row['issue_count']} | {row['total_cycle_time_days']:.1f}d | {row['bottleneck_stage']} |\n"

            # ASCII-style heatmap visualization
            if heatmap_data["transitions"]:
                markdown_content += f"""

### Workflow Stage Breakdown
```
Week {week_number} Cycle Time Breakdown:
"""

                # Create a simple text-based visualization (filter trivial)
                ascii_transitions = [t for t in heatmap_data["transitions"] if t.get("avg_time_days", 0) >= 0.1]
                ascii_transitions = sorted(ascii_transitions, key=lambda x: x["avg_time_hours"], reverse=True)[:6]
                for transition in ascii_transitions:  # Top 6 transitions
                    from_stage = transition["from_stage"][:15]  # Truncate long names
                    to_stage = transition["to_stage"][:15]
                    avg_days = transition["avg_time_days"]

                    # Create a simple bar using characters
                    bar_length = min(int(avg_days * 2), 20)  # Scale down for display
                    bar = "█" * bar_length

                    bottleneck_indicator = (
                        " ⚠️ BOTTLENECK" if transition["to_stage"] == heatmap_data["bottleneck_stage"] else ""
                    )

                    markdown_content += (
                        f"├── {from_stage} → {to_stage}: {avg_days:.1f} days avg {bar}{bottleneck_indicator}\n"
                    )

                markdown_content += "```\n"

        # Performance Analysis
        markdown_content += f"""## 🎯 Performance Analysis

**Flow Efficiency:** {current_week.get("flow_efficiency", 0):.1f}%
**Primary Bottleneck:** {current_week.get("primary_bottleneck", "N/A")}
**Arrival Volatility:** {current_week.get("arrival_volatility", 0):.1f}%

### Flow Status Assessment

"""

        # Add status-specific recommendations
        if net_flow > 5:
            markdown_content += """- 🚨 **Critical:** Severe backlog accumulation detected
- **Action Required:** Immediate flow throttling and bottleneck resolution
- **Timeline:** Address within 1 week to prevent delivery crisis
"""
        elif net_flow > 0:
            markdown_content += """- ⚠️ **At Risk:** Backlog growing moderately
- **Action Required:** Monitor closely and optimize throughput
- **Timeline:** Address within 2 weeks to maintain healthy flow
"""
        else:
            markdown_content += """- ✅ **Healthy:** Flow is balanced or improving
- **Action Required:** Maintain current practices
- **Timeline:** Continue monitoring for trend changes
"""

        # Always add Detailed Issue Analysis section
        # Get arrival and completed issues from the scorecard
        arrival_issues = []
        completed_issues = []

        # Try to get from details first (verbose mode)
        if details:
            arrival_issues = details.get("arrival_issues", [])
            completed_issues = details.get("completed_issues", [])

        # If not in details, check current_week
        if not arrival_issues and "current_week" in scorecard:
            arrival_issues = current_week.get("arrival_issues", [])
        if not completed_issues and "current_week" in scorecard:
            completed_issues = current_week.get("completed_issues", [])

        markdown_content += """

## 📝 Detailed Issue Analysis
"""

        if arrival_issues:
            markdown_content += f"""
### 📥 Arrival Issues ({len(arrival_issues)} total)

*Issues created during the analysis period (contributing to arrival rate)*

| Issue Key | Type | Summary | Created Date | Assignee |
|-----------|------|---------|--------------|----------|
"""

            for issue in arrival_issues[:20]:  # Show more issues
                key = issue.get("key", "N/A")
                issue_type = issue.get("type", "N/A")
                summary = issue.get("summary", "")[:60] + ("..." if len(issue.get("summary", "")) > 60 else "")
                assignee = issue.get("assignee") or "Unassigned"

                # Extract created date from the issue data
                created_date = "N/A"
                if issue.get("created"):
                    created_date_str = issue.get("created", "")
                    # Handle different date formats (ISO with timezone)
                    if "T" in created_date_str:
                        created_date = created_date_str.split("T")[0]  # Get just YYYY-MM-DD part
                    else:
                        created_date = created_date_str[:10]

                markdown_content += f"| {key} | {issue_type} | {summary} | {created_date} | {assignee} |\n"

            if len(arrival_issues) > 20:
                markdown_content += f"\n*... and {len(arrival_issues) - 20} more arrival issues*\n"
        else:
            if details:
                markdown_content += "\n*No arrival issues found in the current analysis period.*\n"
            else:
                markdown_content += "\n*Enable --verbose to include arrival issue details.*\n"

        if completed_issues:
            markdown_content += f"""
### ✅ Completed Issues ({len(completed_issues)} total)

*Issues resolved during the analysis period (contributing to throughput)*

| Issue Key | Type | Summary | Resolved Date | Assignee |
|-----------|------|---------|---------------|----------|
"""

            for issue in completed_issues[:20]:
                key = issue.get("key", "N/A")
                issue_type = issue.get("type", "N/A")
                summary = issue.get("summary", "")[:60] + ("..." if len(issue.get("summary", "")) > 60 else "")
                assignee = issue.get("assignee") or "Unassigned"

                # Extract resolved date from the issue data
                resolved_date = "N/A"
                if issue.get("resolved"):
                    resolved_date_str = issue.get("resolved", "")
                    # Handle different date formats (ISO with timezone)
                    if "T" in resolved_date_str:
                        resolved_date = resolved_date_str.split("T")[0]  # Get just YYYY-MM-DD part
                    else:
                        resolved_date = resolved_date_str[:10]

                markdown_content += f"| {key} | {issue_type} | {summary} | {resolved_date} | {assignee} |\n"

            if len(completed_issues) > 20:
                markdown_content += f"\n*... and {len(completed_issues) - 20} more completed issues*\n"
        else:
            if details:
                markdown_content += "\n*No completed issues found in the current analysis period.*\n"
            else:
                markdown_content += "\n*Enable --verbose to include completed issue details.*\n"

        # Recommendations section
        markdown_content += """

## 💡 Recommendations

### 🚨 Immediate Actions Required
"""

        # Generate contextual recommendations based on current state
        if net_flow > 3:
            markdown_content += """- Implement intake throttling to prevent further backlog growth
- Investigate and resolve primary delivery bottlenecks
- Consider temporary capacity increase or scope reduction
"""
        elif net_flow > 0:
            markdown_content += """- Monitor backlog growth trend closely
- Focus team on completing existing work before new intake
- Review and optimize delivery process efficiency
"""
        else:
            markdown_content += """- Maintain current flow patterns and practices
- Ensure adequate work pipeline for sustained delivery
- Monitor for any emerging bottlenecks or trends
"""

        markdown_content += """
### 🎯 Specific Action Items
"""

        flow_ratio = current_week.get("flow_ratio", 0)
        flow_efficiency = current_week.get("flow_efficiency", 0)

        if flow_ratio < 85:
            markdown_content += f"- **Flow Efficiency:** Ratio at {flow_ratio:.0f}% - target >85%\n"

        if flow_efficiency < 40:
            markdown_content += (
                f"- **Process Optimization:** Flow efficiency at {flow_efficiency:.0f}% - investigate wait times\n"
            )

        # Data Quality & Methodology
        markdown_content += f"""

## 📋 Data Quality & Methodology

### Data Scope
- **Analysis Period:** Week {week_number}
- **Date Range:** {start_date} to {end_date}
- **Team Filter:** {team}
- **Statistical Analysis:** {"Enabled" if statistical_enabled else "Disabled"}

### Methodology
- **Net Flow:** Arrivals - Throughput for the analysis period
- **Flow Ratio:** (Throughput / Arrivals) × 100%
- **Bootstrap CI:** 95% confidence interval using Poisson resampling (B=2000)
- **EWMA:** Exponentially Weighted Moving Average for trend detection
- **Flow Efficiency:** Active time / Total cycle time across workflow states

---

*Report generated on {analysis_date} using PyToolkit JIRA Net Flow Analysis Service*
"""

        return markdown_content

    def _get_flow_status_emoji(self, net_flow: int) -> str:
        """Get emoji for net flow status."""
        if net_flow > 5:
            return "🚨"
        elif net_flow > 0:
            return "⚠️"
        elif net_flow == 0:
            return "✅"
        else:
            return "✅"

    def _get_ratio_status_emoji(self, flow_ratio: float) -> str:
        """Get emoji for flow ratio status."""
        if flow_ratio >= 85:
            return "✅"
        elif flow_ratio >= 70:
            return "⚠️"
        else:
            return "🚨"

    def _get_efficiency_status_emoji(self, flow_efficiency: float) -> str:
        """Get emoji for flow efficiency status."""
        if flow_efficiency >= 40:
            return "✅"
        elif flow_efficiency >= 25:
            return "⚠️"
        else:
            return "🚨"

    def _get_volatility_status_emoji(self, volatility: float) -> str:
        """Get emoji for arrival volatility status."""
        if volatility <= 30:
            return "✅"
        elif volatility <= 50:
            return "⚠️"
        else:
            return "🚨"

    def _get_overall_status_emoji(self, flow_status: str) -> str:
        """Get emoji for overall flow status."""
        status_emojis = {
            "CRITICAL_BOTTLENECK": "🚨",
            "MINOR_BOTTLENECK": "⚠️",
            "HEALTHY_FLOW": "✅",
            "BALANCED": "⚖️",
        }
        return status_emojis.get(flow_status, "❓")
        return status_emojis.get(flow_status, "❓")
