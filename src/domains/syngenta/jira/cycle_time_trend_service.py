"""
JIRA Cycle Time Trend Analysis Service

This service provides trending analysis capabilities for cycle time metrics, including:
1. Automatic baseline period calculation using the "4x rule"
2. Temporal trend analysis with statistical significance testing
3. Pattern detection and early warning alerts
4. Robust statistical methods with outlier detection and handling

FEATURES:
- Baseline calculation: 4x current period duration for historical comparison
- Trend metrics: percentage change, direction, momentum, volatility
- Statistical analysis: linear regression, significance testing
- Outlier detection: IQR-based outlier identification and robust statistics
- Alert system: configurable thresholds with severity levels (INFO, WARNING, CRITICAL)
- Pattern detection: identifies degrading trends and process improvements
- Robust metrics: percentiles (P75, P90, P95) for outlier-resistant analysis

INTEGRATION:
This service integrates with CycleTimeService to provide enhanced analytics
and proactive monitoring capabilities for DevOps and project management teams.
"""

import re
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union

try:
    from scipy import stats  # type: ignore

    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    stats = None

from domains.syngenta.jira.issue_adherence_service import TimePeriodParser
from utils.logging.logging_manager import LogManager


@dataclass
class TrendData:
    """Data structure for trend analysis input."""

    avg_cycle_time: float
    median_cycle_time: float
    sle_compliance: float
    throughput: int
    anomaly_rate: float
    period_start: datetime
    period_end: datetime
    total_issues: int = 0
    issues_with_valid_cycle_time: int = 0
    avg_lead_time: float = 0.0
    median_lead_time: float = 0.0


@dataclass
class TrendResult:
    """Data structure for trend analysis output."""

    metric_name: str
    current_value: float
    baseline_value: float
    change_percent: float
    trend_direction: str  # 'IMPROVING', 'STABLE', 'DEGRADING'
    significance: bool
    confidence_level: float
    trend_slope: float = 0.0
    volatility: float = 0.0
    normalized_baseline: float = 0.0  # Baseline normalized for period comparison
    is_normalized: bool = False  # Whether this metric was normalized


@dataclass
class Alert:
    """Data structure for alert notifications."""

    severity: str  # 'INFO', 'WARNING', 'CRITICAL'
    metric: str
    message: str
    current_value: float
    threshold: float
    recommendation: str
    priority: int = 0  # Lower number = higher priority


@dataclass
class OutlierData:
    """Data structure for outlier analysis results."""

    outliers: List[float]
    outlier_indices: List[int]
    cleaned_data: List[float]
    method: str  # 'IQR', 'Z-SCORE', 'MODIFIED_Z'
    threshold_lower: float
    threshold_upper: float
    outlier_count: int
    outlier_percentage: float


@dataclass
class RobustStats:
    """Data structure for robust statistical metrics."""

    mean: float
    median: float
    trimmed_mean: float  # 20% trimmed mean
    p75: float  # 75th percentile
    p90: float  # 90th percentile
    p95: float  # 95th percentile
    iqr: float  # Interquartile range
    mad: float  # Median Absolute Deviation
    outlier_info: Optional[OutlierData] = None


class OutlierDetector:
    """Statistical outlier detection and robust metrics calculator."""

    @staticmethod
    def detect_outliers_iqr(data: List[float], k: float = 1.5) -> OutlierData:
        """
        Detect outliers using Interquartile Range (IQR) method.

        Args:
            data: List of numeric values
            k: IQR multiplier (1.5 = standard, 3.0 = extreme outliers)

        Returns:
            OutlierData: Complete outlier analysis results
        """
        if len(data) < 4:  # Need at least 4 points for meaningful IQR
            return OutlierData([], [], data, "IQR", 0, 0, 0, 0.0)

        data_sorted = sorted(data)
        q1 = statistics.quantiles(data_sorted, n=4)[0]  # 25th percentile
        q3 = statistics.quantiles(data_sorted, n=4)[2]  # 75th percentile
        iqr = q3 - q1

        lower_bound = q1 - k * iqr
        upper_bound = q3 + k * iqr

        outliers = []
        outlier_indices = []
        cleaned_data = []

        for i, value in enumerate(data):
            if value < lower_bound or value > upper_bound:
                outliers.append(value)
                outlier_indices.append(i)
            else:
                cleaned_data.append(value)

        return OutlierData(
            outliers=outliers,
            outlier_indices=outlier_indices,
            cleaned_data=cleaned_data,
            method="IQR",
            threshold_lower=lower_bound,
            threshold_upper=upper_bound,
            outlier_count=len(outliers),
            outlier_percentage=(len(outliers) / len(data)) * 100 if data else 0.0,
        )

    @staticmethod
    def detect_outliers_modified_z(
        data: List[float], threshold: float = 3.5
    ) -> OutlierData:
        """
        Detect outliers using Modified Z-Score method (more robust than standard Z-score).

        Args:
            data: List of numeric values
            threshold: Modified Z-score threshold (3.5 is recommended)

        Returns:
            OutlierData: Complete outlier analysis results
        """
        if len(data) < 3:
            return OutlierData([], [], data, "MODIFIED_Z", 0, 0, 0, 0.0)

        median = statistics.median(data)
        mad = statistics.median(
            [abs(x - median) for x in data]
        )  # Median Absolute Deviation

        if mad == 0:  # All values are the same
            return OutlierData([], [], data, "MODIFIED_Z", 0, 0, 0, 0.0)

        outliers = []
        outlier_indices = []
        cleaned_data = []

        for i, value in enumerate(data):
            modified_z_score = 0.6745 * (value - median) / mad
            if abs(modified_z_score) > threshold:
                outliers.append(value)
                outlier_indices.append(i)
            else:
                cleaned_data.append(value)

        return OutlierData(
            outliers=outliers,
            outlier_indices=outlier_indices,
            cleaned_data=cleaned_data,
            method="MODIFIED_Z",
            threshold_lower=median - (threshold * mad / 0.6745),
            threshold_upper=median + (threshold * mad / 0.6745),
            outlier_count=len(outliers),
            outlier_percentage=(len(outliers) / len(data)) * 100 if data else 0.0,
        )

    @staticmethod
    def calculate_robust_stats(
        data: List[float], outlier_method: str = "IQR"
    ) -> RobustStats:
        """
        Calculate comprehensive robust statistics with outlier detection.

        Args:
            data: List of numeric values
            outlier_method: 'IQR' or 'MODIFIED_Z'

        Returns:
            RobustStats: Complete robust statistical analysis
        """
        if not data:
            return RobustStats(0, 0, 0, 0, 0, 0, 0, 0)

        data_clean = [
            x for x in data if x is not None and x >= 0
        ]  # Remove null and negative values

        if len(data_clean) < 2:
            single_value = data_clean[0] if data_clean else 0
            return RobustStats(
                single_value,
                single_value,
                single_value,
                single_value,
                single_value,
                single_value,
                0,
                0,
            )

        # Detect outliers
        if outlier_method == "MODIFIED_Z":
            outlier_info = OutlierDetector.detect_outliers_modified_z(data_clean)
        else:
            outlier_info = OutlierDetector.detect_outliers_iqr(data_clean)

        # Use cleaned data (without outliers) for robust statistics
        robust_data = (
            outlier_info.cleaned_data if outlier_info.cleaned_data else data_clean
        )

        # Calculate core statistics
        mean_val = statistics.mean(robust_data)
        median_val = statistics.median(robust_data)

        # Trimmed mean (remove 20% extreme values)
        if len(robust_data) >= 5:
            trim_count = max(1, len(robust_data) // 10)  # 10% from each side
            sorted_data = sorted(robust_data)
            trimmed_data = sorted_data[trim_count:-trim_count]
            trimmed_mean = statistics.mean(trimmed_data) if trimmed_data else mean_val
        else:
            trimmed_mean = mean_val

        # Percentiles
        if len(robust_data) >= 4:
            percentiles = statistics.quantiles(robust_data, n=20)  # 5% increments
            p75 = percentiles[14]  # 75th percentile
            p90 = percentiles[17]  # 90th percentile
            p95 = percentiles[18]  # 95th percentile

            # IQR calculation
            q1 = percentiles[4]  # 25th percentile
            q3 = percentiles[14]  # 75th percentile
            iqr = q3 - q1
        else:
            p75 = p90 = p95 = median_val
            iqr = 0

        # Median Absolute Deviation
        mad = statistics.median([abs(x - median_val) for x in robust_data])

        return RobustStats(
            mean=mean_val,
            median=median_val,
            trimmed_mean=trimmed_mean,
            p75=p75,
            p90=p90,
            p95=p95,
            iqr=iqr,
            mad=mad,
            outlier_info=outlier_info,
        )


class TrendConfig:
    """Configuration settings for trend analysis."""

    DEFAULT_CONFIG = {
        "baseline_multiplier": 4,  # 4x current period for baseline
        # PERCENTAGE-BASED THRESHOLDS: Compare % change between current and baseline
        "trend_thresholds": {
            "stable_range": (-5, 5),  # ±5% change is stable
            "warning_threshold": 15,  # >15% change triggers WARNING
            "critical_threshold": 30,  # >30% change triggers CRITICAL
        },
        "min_data_points": 1,  # Changed from 3 to 1 for single baseline comparison
        "statistical_confidence": 0.95,
        "batch_window_minutes": 5,
        # ROBUST STATISTICS CONFIGURATION
        "outlier_detection": {
            "method": "IQR",  # 'IQR' or 'MODIFIED_Z'
            "iqr_multiplier": 1.5,  # Standard: 1.5, Conservative: 3.0
            "modified_z_threshold": 3.5,  # Modified Z-score threshold
            "enable_outlier_filtering": True,  # Remove outliers for main metrics
            "min_data_points": 4,  # Minimum points needed for outlier detection
        },
        "robust_metrics": {
            "primary_statistic": "median",  # Use median as primary instead of mean
            "use_trimmed_mean": True,  # Enable 20% trimmed mean
            "enable_percentiles": True,  # Calculate P75, P90, P95
            "outlier_threshold_percent": 20,  # Flag if >20% of data are outliers
        },
        # ABSOLUTE VALUE THRESHOLDS: Compare absolute values, not % change
        "sle_thresholds": {
            "critical": 70,  # < 70% compliance (absolute) is critical
            "warning": 80,  # < 80% compliance (absolute) is warning
        },
        "anomaly_thresholds": {
            "critical": 25,  # > 25% anomaly rate (absolute) is critical
            "warning": 15,  # > 15% anomaly rate (absolute) is warning
        },
        # COUNT-BASED THRESHOLDS: Number of metrics degrading
        "degrading_metrics_threshold": 3,  # >= 3 degrading metrics triggers alert
        # LEGACY: May not be used consistently
        "throughput_decline": {
            "critical": 30,  # > 30% decline is critical
            "warning": 15,  # > 15% decline is warning
        },
    }

    def __init__(self, custom_config: Optional[Dict] = None):
        """Initialize configuration with optional custom settings."""
        self.config = self.DEFAULT_CONFIG.copy()
        if custom_config:
            self._merge_config(custom_config)

    def _merge_config(self, custom_config: Dict):
        """Merge custom configuration with defaults."""
        for key, value in custom_config.items():
            if isinstance(value, dict) and key in self.config:
                self.config[key].update(value)
            else:
                self.config[key] = value

    def get(self, key: str, default=None):
        """Get configuration value."""
        return self.config.get(key, default)


class CycleTimeTrendService:
    """Service class for cycle time trend analysis."""

    def __init__(self, config: Optional[TrendConfig] = None):
        """Initialize with optional custom configuration."""
        self.logger = LogManager.get_instance().get_logger("CycleTimeTrendService")
        self.time_parser = TimePeriodParser()
        self.config = config or TrendConfig()

    def calculate_baseline_period(
        self, start_date: Union[datetime, str], end_date: Union[datetime, str]
    ) -> Tuple[datetime, datetime]:
        """
        Calculate baseline period for trend analysis using the 4x rule.

        Rule: baseline duration = 4x current period duration
        The baseline period ends just before the current period starts.

        Args:
            start_date: Current period start date (datetime or ISO string)
            end_date: Current period end date (datetime or ISO string)

        Returns:
            Tuple[datetime, datetime]: Baseline period (start, end) dates

        Raises:
            ValueError: For invalid date inputs or insufficient data scenarios

        Examples:
            >>> service = CycleTimeTrendService()
            >>> # For current period: 2025-01-15 to 2025-01-22 (7 days)
            >>> baseline_start, baseline_end = service.calculate_baseline_period(
            ...     "2025-01-15", "2025-01-22"
            ... )
            >>> # Returns baseline: 2024-12-18 to 2025-01-14 (28 days, 4x7)
        """
        try:
            # Convert string inputs to datetime objects
            if isinstance(start_date, str):
                start_date = self._parse_date_input(start_date)
            if isinstance(end_date, str):
                end_date = self._parse_date_input(end_date)

            # Validate date inputs
            self._validate_date_range(start_date, end_date)

            # Calculate current period duration
            current_duration = end_date - start_date
            baseline_multiplier = self.config.get("baseline_multiplier", 4)

            # Calculate baseline duration (4x current period)
            if isinstance(baseline_multiplier, (int, float)):
                baseline_duration = current_duration * baseline_multiplier
            else:
                baseline_duration = current_duration * 4

            # Baseline period ends just before current period starts
            baseline_end = start_date - timedelta(days=1)
            baseline_start = baseline_end - baseline_duration + timedelta(days=1)

            # Ensure baseline start is not too far in the past (max 1 year)
            min_baseline_start = datetime.now() - timedelta(days=365)
            if baseline_start < min_baseline_start:
                self.logger.warning(
                    f"Baseline start {baseline_start.date()} extends beyond 1 year. "
                    f"Adjusting to {min_baseline_start.date()}"
                )
                baseline_start = min_baseline_start

            # Handle edge cases (weekends, holidays)
            baseline_start, baseline_end = self._adjust_for_business_days(
                baseline_start, baseline_end
            )

            self.logger.info(
                f"Calculated baseline period: {baseline_start.date()} to {baseline_end.date()} "
                f"({baseline_duration.days} days, {baseline_multiplier}x current period)"
            )

            return baseline_start, baseline_end

        except Exception as e:
            self.logger.error(f"Failed to calculate baseline period: {e}")
            raise ValueError(f"Baseline calculation error: {e}")

    def calculate_trend_metrics(
        self, current_data: TrendData, historical_data: List[TrendData]
    ) -> List[TrendResult]:
        """
        Analyze trends comparing current period against historical baseline.

        Performs statistical analysis including:
        - Percentage change calculation
        - Linear regression for trend direction
        - Statistical significance testing
        - Volatility assessment

        Args:
            current_data: Current period metrics
            historical_data: List of historical period metrics for baseline

        Returns:
            List[TrendResult]: Trend analysis results for each metric

        Raises:
            ValueError: For insufficient data or invalid inputs
        """
        try:
            if not historical_data:
                raise ValueError("Historical data is required for trend analysis")

            min_data_points = self.config.get(
                "min_data_points", 1
            )  # Changed from 3 to 1 for single baseline comparison
            if not isinstance(min_data_points, int):
                min_data_points = 1  # Changed from 3 to 1
            if len(historical_data) < min_data_points:
                raise ValueError(
                    f"Insufficient historical data: {len(historical_data)} periods, minimum {min_data_points} required"
                )

            # Calculate current period duration in days for normalization
            current_period_days = (
                current_data.period_end - current_data.period_start
            ).days
            if current_period_days <= 0:
                current_period_days = 1  # Fallback for same-day periods

            # Calculate baseline multiplier and baseline period duration
            baseline_multiplier_raw = self.config.get("baseline_multiplier", 4)
            baseline_multiplier = (
                float(baseline_multiplier_raw)
                if baseline_multiplier_raw is not None
                else 4.0
            )
            baseline_period_days = current_period_days * baseline_multiplier

            self.logger.info(
                f"Analyzing trends for {len(historical_data)} historical periods vs current period"
            )
            self.logger.info(
                f"Current period: {current_period_days} days, Baseline period: {baseline_period_days} days (normalization factor: {baseline_multiplier})"
            )

            # Define metrics to analyze
            metrics_to_analyze = [
                (
                    "avg_cycle_time",
                    "Average Cycle Time",
                    "hours",
                    True,
                ),  # lower is better
                ("median_cycle_time", "Median Cycle Time", "hours", True),
                (
                    "sle_compliance",
                    "SLE Compliance Rate",
                    "%",
                    False,
                ),  # higher is better
                ("throughput", "Throughput", "issues", False),
                ("anomaly_rate", "Anomaly Rate", "%", True),
                ("avg_lead_time", "Average Lead Time", "hours", True),
                ("median_lead_time", "Median Lead Time", "hours", True),
            ]

            trend_results = []

            for metric_attr, metric_name, unit, lower_is_better in metrics_to_analyze:
                try:
                    trend_result = self._analyze_single_metric(
                        current_data,
                        historical_data,
                        metric_attr,
                        metric_name,
                        unit,
                        lower_is_better,
                        baseline_multiplier,
                    )
                    trend_results.append(trend_result)
                except Exception as e:
                    self.logger.warning(f"Failed to analyze {metric_name}: {e}")
                    # Create a default result for failed analysis
                    current_value = getattr(current_data, metric_attr, 0.0)
                    trend_results.append(
                        TrendResult(
                            metric_name=metric_name,
                            current_value=current_value,
                            baseline_value=0.0,
                            change_percent=0.0,
                            trend_direction="UNKNOWN",
                            significance=False,
                            confidence_level=0.0,
                        )
                    )

            self.logger.info(f"Successfully analyzed {len(trend_results)} metrics")
            return trend_results

        except Exception as e:
            self.logger.error(f"Trend analysis failed: {e}")
            raise

    def detect_patterns_and_alerts(
        self,
        trend_data: List[TrendResult],
        current_data: TrendData,
        thresholds: Optional[Dict] = None,
    ) -> List[Alert]:
        """
        Detect patterns and generate early warning alerts.

        Analyzes trend data to identify:
        - Critical performance degradation
        - Warning-level decline patterns
        - Process improvement opportunities
        - Actionable recommendations

        Args:
            trend_data: Results from trend analysis
            current_data: Current period data for context
            thresholds: Optional custom alert thresholds

        Returns:
            List[Alert]: Structured alerts sorted by priority
        """
        try:
            alerts = []
            alert_thresholds = thresholds or self.config.get("trend_thresholds", {})

            self.logger.info(f"Analyzing {len(trend_data)} trend metrics for patterns")

            # Analyze each trend metric
            for trend in trend_data:
                thresholds_dict = (
                    alert_thresholds if isinstance(alert_thresholds, dict) else {}
                )
                metric_alerts = self._analyze_metric_for_alerts(
                    trend, current_data, thresholds_dict
                )
                alerts.extend(metric_alerts)

            # Add holistic pattern analysis
            pattern_alerts = self._analyze_holistic_patterns(trend_data, current_data)
            alerts.extend(pattern_alerts)

            # Sort alerts by priority (lower number = higher priority)
            alerts.sort(key=lambda x: (x.priority, x.severity))

            # Log alert summary
            critical_count = len([a for a in alerts if a.severity == "CRITICAL"])
            warning_count = len([a for a in alerts if a.severity == "WARNING"])
            info_count = len([a for a in alerts if a.severity == "INFO"])

            self.logger.info(
                f"Generated {len(alerts)} alerts: {critical_count} critical, {warning_count} warning, {info_count} info"
            )

            return alerts

        except Exception as e:
            self.logger.error(f"Pattern detection failed: {e}")
            raise

    def _parse_date_input(self, date_input: str) -> datetime:
        """Parse various date input formats."""
        # Handle ISO format strings
        if re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", date_input):
            return datetime.fromisoformat(date_input.replace("Z", "+00:00")).replace(
                tzinfo=None
            )

        # Handle simple date format
        if re.match(r"^\d{4}-\d{2}-\d{2}$", date_input):
            return datetime.strptime(date_input, "%Y-%m-%d")

        # Handle relative periods using existing parser
        try:
            start_date, end_date = self.time_parser.parse_time_period(date_input)
            return start_date  # Return start date for relative periods
        except ValueError:
            pass

        raise ValueError(f"Unable to parse date input: {date_input}")

    def _validate_date_range(self, start_date: datetime, end_date: datetime):
        """Validate date range inputs."""
        if start_date >= end_date:
            raise ValueError("Start date must be before end date")

        # Check for reasonable period duration (not too short or long)
        duration = end_date - start_date
        if duration.days < 1:
            raise ValueError("Period must be at least 1 day")

        if duration.days > 365:
            raise ValueError("Period cannot exceed 1 year")

        # Check if dates are not in the future
        now = datetime.now()
        if start_date > now or end_date > now:
            raise ValueError("Dates cannot be in the future")

    def _adjust_for_business_days(
        self, baseline_start: datetime, baseline_end: datetime
    ) -> Tuple[datetime, datetime]:
        """
        Adjust dates to avoid common edge cases.

        This is a simplified implementation. In production, you might want to:
        - Integrate with a business calendar service
        - Account for company holidays
        - Handle different time zones
        """
        # Simple weekend adjustment - move to next Monday if starting on weekend
        if baseline_start.weekday() == 5:  # Saturday
            baseline_start += timedelta(days=2)
        elif baseline_start.weekday() == 6:  # Sunday
            baseline_start += timedelta(days=1)

        # Ensure we don't overlap with current period
        if baseline_end >= baseline_start:
            return baseline_start, baseline_end
        else:
            # Recalculate if adjustment caused overlap
            duration = baseline_end - baseline_start
            return baseline_start, baseline_start + duration

    def _analyze_single_metric(
        self,
        current_data: TrendData,
        historical_data: List[TrendData],
        metric_attr: str,
        metric_name: str,
        unit: str,
        lower_is_better: bool,
        baseline_multiplier: float = 4.0,
    ) -> TrendResult:
        """Analyze trend for a single metric."""
        # Extract current and historical values
        current_value = getattr(current_data, metric_attr, 0.0)
        historical_values = [
            getattr(data, metric_attr, 0.0) for data in historical_data
        ]

        # Filter out zero/null values for meaningful analysis
        valid_historical = [v for v in historical_values if v > 0]
        if not valid_historical:
            raise ValueError(f"No valid historical data for {metric_name}")

        # Calculate baseline (average of historical values)
        baseline_value = statistics.mean(valid_historical)

        # Normalize baseline for metrics that are affected by period duration
        # Metrics like throughput, total_issues, issues_with_valid_cycle_time should be normalized
        # Rates, percentages, and time-based averages don't need normalization
        normalized_baseline = baseline_value

        # Check if this metric needs period normalization
        # Only COUNT-based metrics need normalization, NOT averages/medians/rates
        metrics_needing_normalization = [
            "throughput",
            "total_issues",
            "issues_with_valid_cycle_time",
        ]

        # NEVER normalize these metrics (they are already averages/medians/rates):
        # - avg_cycle_time, median_cycle_time (time averages)
        # - avg_lead_time, median_lead_time (time averages)
        # - sle_compliance, anomaly_rate (percentages)

        # Verify this metric should actually be normalized
        if metric_attr in [
            "avg_cycle_time",
            "median_cycle_time",
            "avg_lead_time",
            "median_lead_time",
            "sle_compliance",
            "anomaly_rate",
        ]:
            # These are already normalized metrics - don't normalize further
            pass

        if metric_attr in metrics_needing_normalization:
            normalized_baseline = baseline_value / baseline_multiplier
            self.logger.debug(
                f"Normalized {metric_name}: baseline {baseline_value:.1f} -> {normalized_baseline:.1f} (÷{baseline_multiplier})"
            )

        # Calculate percentage change using normalized baseline
        if normalized_baseline > 0:
            change_percent = (
                (current_value - normalized_baseline) / normalized_baseline
            ) * 100
        else:
            change_percent = 0.0

        # Validate for extreme values that might indicate data quality issues
        if abs(change_percent) > 1000 and metric_attr in [
            "avg_cycle_time",
            "median_cycle_time",
            "avg_lead_time",
            "median_lead_time",
        ]:
            self.logger.warning(
                f"Extreme change detected in {metric_name}: {change_percent:.1f}% "
                f"(current: {current_value:.1f}, baseline: {baseline_value:.1f}). "
                f"This may indicate data quality issues or outliers affecting the calculation."
            )

        # Determine trend direction
        trend_direction = self._determine_trend_direction(
            change_percent, lower_is_better
        )

        # Calculate statistical significance using t-test
        if len(valid_historical) >= 3 and SCIPY_AVAILABLE and stats:
            try:
                # Perform one-sample t-test
                t_stat, p_value = stats.ttest_1samp(valid_historical, current_value)
                if isinstance(p_value, (int, float)):
                    confidence_level = 1 - p_value
                    statistical_confidence = self.config.get(
                        "statistical_confidence", 0.95
                    )
                    if isinstance(statistical_confidence, (int, float)):
                        significance = p_value < (1 - statistical_confidence)
                    else:
                        significance = p_value < 0.05
                else:
                    confidence_level = 0.0
                    significance = False
            except Exception as e:
                self.logger.warning(f"Statistical test failed for {metric_name}: {e}")
                confidence_level = 0.0
                significance = False
        else:
            confidence_level = 0.0
            significance = False

        # Calculate volatility (coefficient of variation)
        if len(valid_historical) >= 2:
            volatility = (statistics.stdev(valid_historical) / baseline_value) * 100
        else:
            volatility = 0.0

        # Calculate trend slope using simple calculation
        slope = 0.0
        if len(valid_historical) >= 2:
            # Simple linear trend calculation
            x_values = list(range(len(valid_historical)))
            try:
                # Calculate slope manually using least squares
                n = len(valid_historical)
                sum_x = sum(x_values)
                sum_y = sum(valid_historical)
                sum_xy = sum(x * y for x, y in zip(x_values, valid_historical))
                sum_x2 = sum(x * x for x in x_values)

                denominator = n * sum_x2 - sum_x * sum_x
                if denominator != 0:
                    slope = (n * sum_xy - sum_x * sum_y) / denominator
                else:
                    slope = 0.0
            except Exception as e:
                self.logger.warning(f"Slope calculation failed for {metric_name}: {e}")
                slope = 0.0

        # Check if metric was normalized
        is_normalized = metric_attr in metrics_needing_normalization

        return TrendResult(
            metric_name=metric_name,
            current_value=current_value,
            baseline_value=baseline_value,
            change_percent=change_percent,
            trend_direction=trend_direction,
            significance=significance,
            confidence_level=confidence_level,
            trend_slope=slope,
            volatility=volatility,
            normalized_baseline=normalized_baseline,
            is_normalized=is_normalized,
        )

    def _determine_trend_direction(
        self, change_percent: float, lower_is_better: bool
    ) -> str:
        """Determine trend direction based on change and metric type."""
        trend_thresholds = self.config.get("trend_thresholds", {})
        if isinstance(trend_thresholds, dict):
            stable_range = trend_thresholds.get("stable_range", (-5, 5))
        else:
            stable_range = (-5, 5)

        if stable_range[0] <= change_percent <= stable_range[1]:
            return "STABLE"

        if lower_is_better:
            # For metrics where lower is better (cycle time, anomaly rate)
            return "IMPROVING" if change_percent < 0 else "DEGRADING"
        else:
            # For metrics where higher is better (SLE compliance, throughput)
            return "IMPROVING" if change_percent > 0 else "DEGRADING"

    def _analyze_metric_for_alerts(
        self, trend: TrendResult, current_data: TrendData, thresholds: Dict
    ) -> List[Alert]:
        """Analyze a single metric for alert conditions."""
        alerts = []

        warning_threshold = thresholds.get("warning_threshold", 15)
        critical_threshold = thresholds.get("critical_threshold", 30)

        # Check for significant changes
        abs_change = abs(trend.change_percent)

        if abs_change >= critical_threshold and trend.significance:
            severity = "CRITICAL"
            priority = 1
            recommendation = self._get_critical_recommendation(trend)
        elif abs_change >= warning_threshold:
            severity = "WARNING"
            priority = 2
            recommendation = self._get_warning_recommendation(trend)
        elif trend.trend_direction != "STABLE":
            severity = "INFO"
            priority = 3
            recommendation = self._get_info_recommendation(trend)
        else:
            return alerts  # No alert needed

        alert = Alert(
            severity=severity,
            metric=trend.metric_name,
            message=f"{trend.metric_name} {trend.trend_direction.lower()} by {abs_change:.1f}%",
            current_value=trend.current_value,
            threshold=warning_threshold
            if severity == "WARNING"
            else critical_threshold,
            recommendation=recommendation,
            priority=priority,
        )

        alerts.append(alert)
        return alerts

    def _analyze_holistic_patterns(
        self, trend_data: List[TrendResult], current_data: TrendData
    ) -> List[Alert]:
        """Analyze patterns across multiple metrics."""
        alerts = []

        # Check SLE compliance
        sle_trend = next(
            (t for t in trend_data if "SLE Compliance" in t.metric_name), None
        )
        if sle_trend:
            sle_alerts = self._check_sle_compliance_alerts(sle_trend, current_data)
            alerts.extend(sle_alerts)

        # Check anomaly rate
        anomaly_trend = next(
            (t for t in trend_data if "Anomaly Rate" in t.metric_name), None
        )
        if anomaly_trend:
            anomaly_alerts = self._check_anomaly_rate_alerts(
                anomaly_trend, current_data
            )
            alerts.extend(anomaly_alerts)

        # Check for degrading performance across multiple metrics
        degrading_metrics = [t for t in trend_data if t.trend_direction == "DEGRADING"]
        degrading_threshold = self.config.get("degrading_metrics_threshold", 3)
        if not isinstance(degrading_threshold, int):
            degrading_threshold = 3
        if len(degrading_metrics) >= degrading_threshold:
            alerts.append(
                Alert(
                    severity="CRITICAL",
                    metric="Multiple Metrics",
                    message=f"{len(degrading_metrics)} metrics showing degradation",
                    current_value=len(degrading_metrics),
                    threshold=degrading_threshold,
                    recommendation="Immediate process review required. Multiple performance indicators declining.",
                    priority=0,  # Highest priority
                )
            )

        return alerts

    def _check_sle_compliance_alerts(
        self, sle_trend: TrendResult, current_data: TrendData
    ) -> List[Alert]:
        """Check SLE compliance specific alerts."""
        alerts = []
        sle_thresholds_config = self.config.get("sle_thresholds", {})
        sle_thresholds = (
            sle_thresholds_config if isinstance(sle_thresholds_config, dict) else {}
        )

        compliance_rate = sle_trend.current_value

        critical_threshold = (
            sle_thresholds.get("critical", 70) if sle_thresholds else 70
        )
        warning_threshold = sle_thresholds.get("warning", 80) if sle_thresholds else 80

        if compliance_rate < critical_threshold:
            alerts.append(
                Alert(
                    severity="CRITICAL",
                    metric="SLE Compliance",
                    message=f"SLE compliance critically low at {compliance_rate:.1f}%",
                    current_value=compliance_rate,
                    threshold=critical_threshold,
                    recommendation="Immediate escalation required. Review and optimize resolution processes.",
                    priority=1,
                )
            )
        elif compliance_rate < warning_threshold:
            alerts.append(
                Alert(
                    severity="WARNING",
                    metric="SLE Compliance",
                    message=f"SLE compliance below target at {compliance_rate:.1f}%",
                    current_value=compliance_rate,
                    threshold=warning_threshold,
                    recommendation="Review processes and identify bottlenecks in next sprint planning.",
                    priority=2,
                )
            )

        return alerts

    def _check_anomaly_rate_alerts(
        self, anomaly_trend: TrendResult, current_data: TrendData
    ) -> List[Alert]:
        """Check anomaly rate specific alerts."""
        alerts = []
        anomaly_thresholds_config = self.config.get("anomaly_thresholds", {})
        anomaly_thresholds = (
            anomaly_thresholds_config
            if isinstance(anomaly_thresholds_config, dict)
            else {}
        )

        anomaly_rate = anomaly_trend.current_value

        critical_threshold = (
            anomaly_thresholds.get("critical", 25) if anomaly_thresholds else 25
        )
        warning_threshold = (
            anomaly_thresholds.get("warning", 15) if anomaly_thresholds else 15
        )

        if anomaly_rate > critical_threshold:
            alerts.append(
                Alert(
                    severity="CRITICAL",
                    metric="Anomaly Rate",
                    message=f"Anomaly rate critically high at {anomaly_rate:.1f}%",
                    current_value=anomaly_rate,
                    threshold=critical_threshold,
                    recommendation="Investigate batch updates and admin closures. Review data quality.",
                    priority=1,
                )
            )
        elif anomaly_rate > warning_threshold:
            alerts.append(
                Alert(
                    severity="WARNING",
                    metric="Anomaly Rate",
                    message=f"Anomaly rate elevated at {anomaly_rate:.1f}%",
                    current_value=anomaly_rate,
                    threshold=warning_threshold,
                    recommendation="Monitor data quality and review issue closure patterns.",
                    priority=2,
                )
            )

        return alerts

    def _get_critical_recommendation(self, trend: TrendResult) -> str:
        """Get recommendation for critical alerts."""
        if "Cycle Time" in trend.metric_name and trend.trend_direction == "DEGRADING":
            return "Immediate process review required. Identify and remove blockers urgently."
        elif (
            "SLE Compliance" in trend.metric_name
            and trend.trend_direction == "DEGRADING"
        ):
            return "Escalate to management. Review SLE targets and resource allocation."
        elif "Throughput" in trend.metric_name and trend.trend_direction == "DEGRADING":
            return "Critical capacity issue. Review workload distribution and team availability."
        else:
            return "Immediate attention required. Review underlying processes and take corrective action."

    def _get_warning_recommendation(self, trend: TrendResult) -> str:
        """Get recommendation for warning alerts."""
        if "Cycle Time" in trend.metric_name:
            return "Review recent changes and identify potential bottlenecks in next sprint."
        elif "SLE Compliance" in trend.metric_name:
            return "Monitor closely and implement process improvements."
        elif "Throughput" in trend.metric_name:
            return "Review team capacity and upcoming deadlines."
        else:
            return "Monitor trend and consider process adjustments."

    def _get_info_recommendation(self, trend: TrendResult) -> str:
        """Get recommendation for info alerts."""
        if trend.trend_direction == "IMPROVING":
            return "Positive trend detected. Document successful practices for replication."
        else:
            return "Minor trend change detected. Continue monitoring."

    def convert_cycle_time_data_to_trend_data(
        self, cycle_time_result: Dict
    ) -> TrendData:
        """
        Convert CycleTimeService result to TrendData format.

        This helper method bridges the existing CycleTimeService output
        with the new trending analysis input format.
        """
        try:
            metadata = cycle_time_result.get("analysis_metadata", {})
            metrics = cycle_time_result.get("metrics", {})

            # Parse dates from metadata
            start_date = datetime.fromisoformat(metadata.get("start_date", ""))
            end_date = datetime.fromisoformat(metadata.get("end_date", ""))

            # Convert metrics
            trend_data = TrendData(
                avg_cycle_time=metrics.get("average_cycle_time_hours", 0.0),
                median_cycle_time=metrics.get("median_cycle_time_hours", 0.0),
                sle_compliance=self._calculate_sle_compliance_rate(cycle_time_result),
                throughput=metrics.get("total_issues", 0),
                anomaly_rate=metrics.get("anomaly_percentage", 0.0),
                period_start=start_date,
                period_end=end_date,
                total_issues=metrics.get("total_issues", 0),
                issues_with_valid_cycle_time=metrics.get(
                    "issues_with_valid_cycle_time", 0
                ),
                avg_lead_time=metrics.get("average_lead_time_hours", 0.0),
                median_lead_time=metrics.get("median_lead_time_hours", 0.0),
            )

            return trend_data

        except Exception as e:
            self.logger.error(f"Failed to convert cycle time data: {e}")
            raise ValueError(f"Data conversion error: {e}")

    def _calculate_sle_compliance_rate(self, cycle_time_result: Dict) -> float:
        """
        Calculate SLE compliance rate from cycle time analysis results.

        SLE compliance is calculated only for Bug and Support issue types,
        as these are the types where SLE/SLA targets apply.

        This is a simplified calculation. In production, you might want to:
        - Use actual SLE targets from configuration
        - Consider different SLE targets per priority
        - Include lead time compliance
        """
        try:
            issues = cycle_time_result.get("issues", [])
            if not issues:
                return 0.0

            # Filter issues to only Bug and Support types for SLE compliance
            sle_applicable_issues = [
                issue
                for issue in issues
                if issue.get("issue_type", "").lower() in ["bug", "support"]
            ]

            if not sle_applicable_issues:
                self.logger.info(
                    "No Bug or Support issues found for SLE compliance calculation"
                )
                self.logger.info(
                    f"Available issue types: {[issue.get('issue_type') for issue in issues[:5]]}"
                )
                return 0.0

            compliant_issues = 0
            total_sle_issues = len(sle_applicable_issues)

            for issue in sle_applicable_issues:
                cycle_time_hours = issue.get("cycle_time_hours", 0)
                priority = issue.get("priority", "")

                # Simple SLE targets (customize based on your requirements)
                if "critical" in priority.lower() or "p1" in priority.lower():
                    sle_target = 24  # 24 hours for critical
                elif "high" in priority.lower() or "p2" in priority.lower():
                    sle_target = 72  # 72 hours for high
                else:
                    sle_target = 168  # 168 hours (1 week) for others

                if cycle_time_hours <= sle_target:
                    compliant_issues += 1

            compliance_rate = (compliant_issues / total_sle_issues) * 100
            self.logger.info(
                f"SLE compliance calculated: {compliant_issues}/{total_sle_issues} Bug/Support issues compliant ({compliance_rate:.1f}%)"
            )
            return compliance_rate

        except Exception as e:
            self.logger.warning(f"Could not calculate SLE compliance: {e}")
            return 0.0
