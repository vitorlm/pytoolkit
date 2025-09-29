"""Configuration system for Datadog observability analysis with tuneable parameters."""

from __future__ import annotations

import os
import yaml
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Dict, Any, Optional

from utils.logging.logging_manager import LogManager


@dataclass
class FlappingConfig:
    """Configuration for flapping detection algorithms."""

    # Time window for detecting flapping cycles
    flap_window_minutes: int = 60

    # Minimum number of cycles to consider flapping
    flap_min_cycles: int = 3

    # Maximum transitions per hour threshold
    flap_max_transitions_per_hour: int = 10

    # Coefficient of variation threshold for metric oscillation
    flap_cv_threshold: float = 0.5

    # Enable advanced flapping detection (consider metric patterns)
    enable_advanced_flapping: bool = False


@dataclass
class TransientConfig:
    """Configuration for benign transient detection."""

    # Maximum duration for a cycle to be considered transient
    transient_max_duration_seconds: float = 300.0  # 5 minutes

    # Time window to check for repeated state flips
    transient_window_seconds: float = 3600.0  # 1 hour

    # Minimum gap between transients to avoid clustering
    transient_min_gap_seconds: float = 1800.0  # 30 minutes

    # Require simple state transition (alert -> recover only)
    require_simple_transition: bool = True

    # Consider business hours in classification
    consider_business_hours: bool = True


@dataclass
class ActionableConfig:
    """Configuration for actionable alert detection."""

    # Minimum duration for actionable alerts
    actionable_min_duration_seconds: float = 600.0  # 10 minutes

    # Minimum time-to-live for consideration
    actionable_min_ttl_seconds: float = 120.0  # 2 minutes

    # Weight given to human action indicators
    human_action_weight: float = 0.4

    # Weight given to business hours occurrence
    business_hours_weight: float = 0.15


@dataclass
class BusinessHoursConfig:
    """Configuration for business hours analysis."""

    # Business hours start (24-hour format)
    start_hour: int = 9  # 9 AM

    # Business hours end (24-hour format)
    end_hour: int = 17  # 5 PM

    # Timezone for business hours (Brazilian timezone)
    timezone_offset_hours: int = -3  # UTC-3 (BRT/BRST)

    # Consider weekends as non-business time
    exclude_weekends: bool = True

    # Custom business days (0=Monday, 6=Sunday)
    business_days: list = field(default_factory=lambda: [0, 1, 2, 3, 4])  # Mon-Fri


@dataclass
class TrendAnalysisConfig:
    """Configuration for temporal trend analysis."""

    # Minimum number of weeks required for trend analysis
    min_weeks_for_trends: int = 3

    # Default lookback period for trend analysis
    default_lookback_weeks: int = 8

    # Maximum weeks to retain in storage
    max_retention_weeks: int = 12

    # Minimum significance threshold for trend detection
    min_trend_significance: float = 0.3

    # Slope threshold for considering changes significant
    slope_significance_threshold: float = 0.01

    # Percentage change threshold for notable changes
    notable_change_threshold: float = 20.0

    # Enable automatic trend alerts
    enable_trend_alerts: bool = False


@dataclass
class ThresholdConfig:
    """Configuration for various analysis thresholds."""

    # Noise score thresholds
    noise_score_high_threshold: float = 80.0
    noise_score_medium_threshold: float = 60.0

    # Self-healing rate thresholds
    self_heal_high_threshold: float = 0.8  # 80%
    self_heal_low_threshold: float = 0.2   # 20%

    # Health score thresholds for grades
    health_score_a_threshold: float = 80.0  # Grade A
    health_score_b_threshold: float = 70.0  # Grade B
    health_score_c_threshold: float = 60.0  # Grade C
    health_score_d_threshold: float = 50.0  # Grade D
    # Below 50 = Grade F

    # Confidence thresholds for recommendations
    high_confidence_threshold: float = 0.85
    medium_confidence_threshold: float = 0.70
    low_confidence_threshold: float = 0.50


@dataclass
class HysteresisConfig:
    """Configuration for hysteresis and debounce recommendations."""

    # Enable hysteresis recommendations
    enable_hysteresis: bool = False

    # Multipliers for hysteresis thresholds
    up_threshold_multiplier: float = 1.05    # 5% higher for alerts
    down_threshold_multiplier: float = 0.95  # 5% lower for recovery

    # Default debounce window recommendations
    default_debounce_seconds: float = 60.0   # 1 minute
    max_debounce_seconds: float = 300.0      # 5 minutes

    # Enable debounce recommendations
    enable_debounce_recommendations: bool = True


@dataclass
class ObservabilityConfig:
    """Main configuration class for observability analysis."""

    # Sub-configurations
    flapping: FlappingConfig = field(default_factory=FlappingConfig)
    transient: TransientConfig = field(default_factory=TransientConfig)
    actionable: ActionableConfig = field(default_factory=ActionableConfig)
    business_hours: BusinessHoursConfig = field(default_factory=BusinessHoursConfig)
    trend_analysis: TrendAnalysisConfig = field(default_factory=TrendAnalysisConfig)
    thresholds: ThresholdConfig = field(default_factory=ThresholdConfig)
    hysteresis: HysteresisConfig = field(default_factory=HysteresisConfig)

    # Global settings
    analysis_period_days: int = 30
    enable_advanced_analysis: bool = True
    enable_trend_analysis: bool = True

    # Snapshot storage settings
    snapshots_dir: str = "snapshots"
    enable_snapshot_cleanup: bool = True

    # Reporting settings
    include_recommendations: bool = True
    max_recommendations_per_type: int = 10

    @classmethod
    def load_from_file(cls, config_path: str) -> 'ObservabilityConfig':
        """Load configuration from YAML file."""
        config_file = Path(config_path)

        if not config_file.exists():
            logger = LogManager.get_instance().get_logger("ObservabilityConfig")
            logger.warning(f"Config file not found: {config_path}, using defaults")
            return cls()

        try:
            with open(config_file, 'r') as f:
                config_data = yaml.safe_load(f) or {}

            return cls._from_dict(config_data)

        except Exception as e:
            logger = LogManager.get_instance().get_logger("ObservabilityConfig")
            logger.error(f"Failed to load config from {config_path}: {e}")
            logger.info("Using default configuration")
            return cls()

    @classmethod
    def load_from_env(cls) -> 'ObservabilityConfig':
        """Load configuration from environment variables."""
        config = cls()

        # Load from environment with OBSERVABILITY_ prefix
        env_mappings = {
            'OBSERVABILITY_FLAP_WINDOW_MINUTES': ('flapping', 'flap_window_minutes', int),
            'OBSERVABILITY_FLAP_MIN_CYCLES': ('flapping', 'flap_min_cycles', int),
            'OBSERVABILITY_TRANSIENT_MAX_DURATION': ('transient', 'transient_max_duration_seconds', float),
            'OBSERVABILITY_ACTIONABLE_MIN_DURATION': ('actionable', 'actionable_min_duration_seconds', float),
            'OBSERVABILITY_BUSINESS_HOURS_START': ('business_hours', 'start_hour', int),
            'OBSERVABILITY_BUSINESS_HOURS_END': ('business_hours', 'end_hour', int),
            'OBSERVABILITY_MIN_WEEKS_FOR_TRENDS': ('trend_analysis', 'min_weeks_for_trends', int),
            'OBSERVABILITY_LOOKBACK_WEEKS': ('trend_analysis', 'default_lookback_weeks', int),
            'OBSERVABILITY_NOISE_HIGH_THRESHOLD': ('thresholds', 'noise_score_high_threshold', float),
            'OBSERVABILITY_SELF_HEAL_HIGH_THRESHOLD': ('thresholds', 'self_heal_high_threshold', float),
            'OBSERVABILITY_ENABLE_HYSTERESIS': ('hysteresis', 'enable_hysteresis', bool),
            'OBSERVABILITY_DEBOUNCE_SECONDS': ('hysteresis', 'default_debounce_seconds', float),
        }

        for env_var, (section, key, type_func) in env_mappings.items():
            value = os.getenv(env_var)
            if value is not None:
                try:
                    typed_value = type_func(value)
                    section_obj = getattr(config, section)
                    setattr(section_obj, key, typed_value)
                except (ValueError, TypeError) as e:
                    logger = LogManager.get_instance().get_logger("ObservabilityConfig")
                    logger.warning(f"Invalid value for {env_var}: {value}, error: {e}")

        return config

    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> 'ObservabilityConfig':
        """Create configuration from dictionary."""
        config = cls()

        # Update flapping config
        if 'flapping' in data:
            flapping_data = data['flapping']
            for key, value in flapping_data.items():
                if hasattr(config.flapping, key):
                    setattr(config.flapping, key, value)

        # Update transient config
        if 'transient' in data:
            transient_data = data['transient']
            for key, value in transient_data.items():
                if hasattr(config.transient, key):
                    setattr(config.transient, key, value)

        # Update actionable config
        if 'actionable' in data:
            actionable_data = data['actionable']
            for key, value in actionable_data.items():
                if hasattr(config.actionable, key):
                    setattr(config.actionable, key, value)

        # Update business hours config
        if 'business_hours' in data:
            bh_data = data['business_hours']
            for key, value in bh_data.items():
                if hasattr(config.business_hours, key):
                    setattr(config.business_hours, key, value)

        # Update trend analysis config
        if 'trend_analysis' in data:
            trend_data = data['trend_analysis']
            for key, value in trend_data.items():
                if hasattr(config.trend_analysis, key):
                    setattr(config.trend_analysis, key, value)

        # Update thresholds config
        if 'thresholds' in data:
            threshold_data = data['thresholds']
            for key, value in threshold_data.items():
                if hasattr(config.thresholds, key):
                    setattr(config.thresholds, key, value)

        # Update hysteresis config
        if 'hysteresis' in data:
            hysteresis_data = data['hysteresis']
            for key, value in hysteresis_data.items():
                if hasattr(config.hysteresis, key):
                    setattr(config.hysteresis, key, value)

        # Update global settings
        global_keys = [
            'analysis_period_days', 'enable_advanced_analysis', 'enable_trend_analysis',
            'snapshots_dir', 'enable_snapshot_cleanup', 'include_recommendations',
            'max_recommendations_per_type'
        ]

        for key in global_keys:
            if key in data:
                setattr(config, key, data[key])

        return config

    def save_to_file(self, config_path: str) -> None:
        """Save current configuration to YAML file."""
        config_file = Path(config_path)
        config_file.parent.mkdir(parents=True, exist_ok=True)

        try:
            config_dict = asdict(self)
            with open(config_file, 'w') as f:
                yaml.dump(config_dict, f, default_flow_style=False, indent=2)

            logger = LogManager.get_instance().get_logger("ObservabilityConfig")
            logger.info(f"Configuration saved to {config_path}")

        except Exception as e:
            logger = LogManager.get_instance().get_logger("ObservabilityConfig")
            logger.error(f"Failed to save config to {config_path}: {e}")
            raise

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return asdict(self)

    def validate(self) -> None:
        """Validate configuration values."""
        errors = []

        # Validate flapping config
        if self.flapping.flap_window_minutes <= 0:
            errors.append("flapping.flap_window_minutes must be positive")
        if self.flapping.flap_min_cycles < 2:
            errors.append("flapping.flap_min_cycles must be at least 2")

        # Validate transient config
        if self.transient.transient_max_duration_seconds <= 0:
            errors.append("transient.transient_max_duration_seconds must be positive")

        # Validate actionable config
        if self.actionable.actionable_min_duration_seconds <= 0:
            errors.append("actionable.actionable_min_duration_seconds must be positive")

        # Validate business hours
        if not (0 <= self.business_hours.start_hour <= 23):
            errors.append("business_hours.start_hour must be between 0 and 23")
        if not (0 <= self.business_hours.end_hour <= 23):
            errors.append("business_hours.end_hour must be between 0 and 23")
        if self.business_hours.start_hour >= self.business_hours.end_hour:
            errors.append("business_hours.start_hour must be less than end_hour")

        # Validate trend analysis
        if self.trend_analysis.min_weeks_for_trends < 2:
            errors.append("trend_analysis.min_weeks_for_trends must be at least 2")
        if self.trend_analysis.default_lookback_weeks < self.trend_analysis.min_weeks_for_trends:
            errors.append("trend_analysis.default_lookback_weeks must be >= min_weeks_for_trends")

        # Validate thresholds
        if not (0 <= self.thresholds.self_heal_high_threshold <= 1):
            errors.append("thresholds.self_heal_high_threshold must be between 0 and 1")
        if not (0 <= self.thresholds.self_heal_low_threshold <= 1):
            errors.append("thresholds.self_heal_low_threshold must be between 0 and 1")

        if errors:
            raise ValueError(f"Configuration validation failed: {', '.join(errors)}")


def load_config(config_path: Optional[str] = None) -> ObservabilityConfig:
    """Load configuration with fallback hierarchy."""

    # Try specific path first
    if config_path:
        return ObservabilityConfig.load_from_file(config_path)

    # Try standard locations
    standard_paths = [
        "config/observability.yml",
        "config/observability.yaml",
        "src/domains/syngenta/datadog/config.yml",
        "src/domains/syngenta/datadog/config.yaml",
        "observability.yml",
        "observability.yaml"
    ]

    for path in standard_paths:
        if Path(path).exists():
            return ObservabilityConfig.load_from_file(path)

    # Try environment variables
    config = ObservabilityConfig.load_from_env()

    # Validate and return
    config.validate()
    return config


# Create example configuration file
EXAMPLE_CONFIG_YAML = """
# Observability Analysis Configuration
# All settings are optional - defaults will be used if not specified

# Flapping detection settings
flapping:
  flap_window_minutes: 60           # Time window for detecting cycles
  flap_min_cycles: 3                # Minimum cycles to consider flapping
  flap_max_transitions_per_hour: 10 # Transition rate threshold
  flap_cv_threshold: 0.5            # Coefficient of variation threshold
  enable_advanced_flapping: false   # Use advanced pattern detection

# Benign transient detection settings
transient:
  transient_max_duration_seconds: 300.0  # 5 minutes max for transients
  transient_window_seconds: 3600.0       # 1 hour window check
  transient_min_gap_seconds: 1800.0      # 30 minutes between transients
  require_simple_transition: true        # Only alert->recover transitions
  consider_business_hours: true          # Factor in business hours

# Actionable alert settings
actionable:
  actionable_min_duration_seconds: 600.0  # 10 minutes minimum
  actionable_min_ttl_seconds: 120.0       # 2 minutes time-to-live
  human_action_weight: 0.4                # Weight for human action indicators
  business_hours_weight: 0.15             # Weight for business hours

# Business hours configuration (Brazilian timezone)
business_hours:
  start_hour: 9                     # 9 AM
  end_hour: 17                      # 5 PM
  timezone_offset_hours: -3         # UTC-3 (BRT/BRST)
  exclude_weekends: true            # Exclude Sat/Sun
  business_days: [0, 1, 2, 3, 4]   # Monday-Friday

# Trend analysis settings
trend_analysis:
  min_weeks_for_trends: 3           # Minimum weeks needed
  default_lookback_weeks: 8         # Default analysis period
  max_retention_weeks: 12           # Storage retention
  min_trend_significance: 0.3       # Statistical significance threshold
  slope_significance_threshold: 0.01 # Change magnitude threshold
  notable_change_threshold: 20.0     # Percentage change for notifications
  enable_trend_alerts: false        # Automated trend alerting

# Analysis thresholds
thresholds:
  noise_score_high_threshold: 80.0      # High noise threshold
  noise_score_medium_threshold: 60.0    # Medium noise threshold
  self_heal_high_threshold: 0.8         # 80% self-healing rate
  self_heal_low_threshold: 0.2          # 20% self-healing rate
  health_score_a_threshold: 80.0        # Grade A cutoff
  health_score_b_threshold: 70.0        # Grade B cutoff
  health_score_c_threshold: 60.0        # Grade C cutoff
  health_score_d_threshold: 50.0        # Grade D cutoff
  high_confidence_threshold: 0.85       # High confidence recommendations
  medium_confidence_threshold: 0.70     # Medium confidence
  low_confidence_threshold: 0.50       # Low confidence

# Hysteresis and debounce recommendations
hysteresis:
  enable_hysteresis: false               # Enable hysteresis suggestions
  up_threshold_multiplier: 1.05         # 5% higher for alerts
  down_threshold_multiplier: 0.95       # 5% lower for recovery
  default_debounce_seconds: 60.0        # 1 minute default debounce
  max_debounce_seconds: 300.0           # 5 minutes maximum
  enable_debounce_recommendations: true  # Include debounce suggestions

# Global settings
analysis_period_days: 30              # Analysis window
enable_advanced_analysis: true        # Advanced metrics and ML
enable_trend_analysis: true           # Temporal trend analysis
snapshots_dir: "snapshots"           # Snapshot storage directory
enable_snapshot_cleanup: true        # Automatic cleanup of old snapshots
include_recommendations: true        # Include actionable recommendations
max_recommendations_per_type: 10     # Limit recommendations per category
"""


def create_example_config(output_path: str = "config/observability.yml") -> None:
    """Create an example configuration file."""
    config_file = Path(output_path)
    config_file.parent.mkdir(parents=True, exist_ok=True)

    with open(config_file, 'w') as f:
        f.write(EXAMPLE_CONFIG_YAML)

    logger = LogManager.get_instance().get_logger("ObservabilityConfig")
    logger.info(f"Example configuration created at {output_path}")