"""SonarQube Metrics Registry

Central registry for SonarQube metric definitions, providing standardized
naming, descriptions, and validation schemas for all SonarQube operations.
"""

from dataclasses import dataclass
from enum import Enum


class MetricType(Enum):
    """Types of SonarQube metrics."""

    QUALITY = "quality"
    SECURITY = "security"
    MAINTAINABILITY = "maintainability"
    RELIABILITY = "reliability"
    COVERAGE = "coverage"
    SIZE = "size"
    COMPLEXITY = "complexity"
    DUPLICATION = "duplication"


class MetricLevel(Enum):
    """Metric aggregation levels."""

    PROJECT = "project"
    ORGANIZATION = "organization"
    PORTFOLIO = "portfolio"


@dataclass
class MetricDefinition:
    """Definition of a SonarQube metric."""

    key: str
    name: str
    type: MetricType
    level: MetricLevel
    unit: str
    description: str
    higher_is_better: bool = False
    numeric: bool = True


class SonarQubeMetricsRegistry:
    """Central registry for SonarQube metric definitions.

    Provides standardized metric naming, descriptions, and validation
    for all SonarQube operations across the PyToolkit framework.
    """

    # Core quality metrics (most commonly used)
    CORE_QUALITY_METRICS: dict[str, MetricDefinition] = {
        "alert_status": MetricDefinition(
            key="alert_status",
            name="Quality Gate Status",
            type=MetricType.QUALITY,
            level=MetricLevel.PROJECT,
            unit="status",
            description="Overall quality gate pass/fail status",
            numeric=False,
        ),
        "bugs": MetricDefinition(
            key="bugs",
            name="Bugs",
            type=MetricType.RELIABILITY,
            level=MetricLevel.PROJECT,
            unit="issues",
            description="Number of bug issues",
        ),
        "vulnerabilities": MetricDefinition(
            key="vulnerabilities",
            name="Vulnerabilities",
            type=MetricType.SECURITY,
            level=MetricLevel.PROJECT,
            unit="issues",
            description="Number of vulnerability issues",
        ),
        "code_smells": MetricDefinition(
            key="code_smells",
            name="Code Smells",
            type=MetricType.MAINTAINABILITY,
            level=MetricLevel.PROJECT,
            unit="issues",
            description="Number of maintainability issues",
        ),
        "coverage": MetricDefinition(
            key="coverage",
            name="Test Coverage",
            type=MetricType.COVERAGE,
            level=MetricLevel.PROJECT,
            unit="%",
            description="Percentage of code covered by tests",
            higher_is_better=True,
        ),
        "duplicated_lines_density": MetricDefinition(
            key="duplicated_lines_density",
            name="Duplicated Lines",
            type=MetricType.DUPLICATION,
            level=MetricLevel.PROJECT,
            unit="%",
            description="Percentage of duplicated lines",
        ),
        "ncloc": MetricDefinition(
            key="ncloc",
            name="Lines of Code",
            type=MetricType.SIZE,
            level=MetricLevel.PROJECT,
            unit="lines",
            description="Non-commented lines of code",
        ),
    }

    # Security metrics
    SECURITY_METRICS: dict[str, MetricDefinition] = {
        "security_rating": MetricDefinition(
            key="security_rating",
            name="Security Rating",
            type=MetricType.SECURITY,
            level=MetricLevel.PROJECT,
            unit="rating",
            description="Security rating (A-E scale)",
            numeric=False,
        ),
        "security_hotspots_reviewed": MetricDefinition(
            key="security_hotspots_reviewed",
            name="Security Hotspots Reviewed",
            type=MetricType.SECURITY,
            level=MetricLevel.PROJECT,
            unit="%",
            description="Percentage of security hotspots reviewed",
            higher_is_better=True,
        ),
        "security_review_rating": MetricDefinition(
            key="security_review_rating",
            name="Security Review Rating",
            type=MetricType.SECURITY,
            level=MetricLevel.PROJECT,
            unit="rating",
            description="Security review rating (A-E scale)",
            numeric=False,
        ),
        "security_issues": MetricDefinition(
            key="security_issues",
            name="Security Issues",
            type=MetricType.SECURITY,
            level=MetricLevel.PROJECT,
            unit="issues",
            description="Total number of security-related issues",
        ),
    }

    # Reliability metrics
    RELIABILITY_METRICS: dict[str, MetricDefinition] = {
        "reliability_rating": MetricDefinition(
            key="reliability_rating",
            name="Reliability Rating",
            type=MetricType.RELIABILITY,
            level=MetricLevel.PROJECT,
            unit="rating",
            description="Reliability rating (A-E scale)",
            numeric=False,
        ),
        "reliability_issues": MetricDefinition(
            key="reliability_issues",
            name="Reliability Issues",
            type=MetricType.RELIABILITY,
            level=MetricLevel.PROJECT,
            unit="issues",
            description="Total number of reliability-related issues",
        ),
    }

    # Maintainability metrics
    MAINTAINABILITY_METRICS: dict[str, MetricDefinition] = {
        "sqale_rating": MetricDefinition(
            key="sqale_rating",
            name="Maintainability Rating",
            type=MetricType.MAINTAINABILITY,
            level=MetricLevel.PROJECT,
            unit="rating",
            description="Maintainability rating (A-E scale)",
            numeric=False,
        ),
        "sqale_index": MetricDefinition(
            key="sqale_index",
            name="Technical Debt",
            type=MetricType.MAINTAINABILITY,
            level=MetricLevel.PROJECT,
            unit="minutes",
            description="Technical debt in minutes",
        ),
        "maintainability_issues": MetricDefinition(
            key="maintainability_issues",
            name="Maintainability Issues",
            type=MetricType.MAINTAINABILITY,
            level=MetricLevel.PROJECT,
            unit="issues",
            description="Total number of maintainability-related issues",
        ),
    }

    # Size and complexity metrics
    SIZE_COMPLEXITY_METRICS: dict[str, MetricDefinition] = {
        "lines": MetricDefinition(
            key="lines",
            name="Total Lines",
            type=MetricType.SIZE,
            level=MetricLevel.PROJECT,
            unit="lines",
            description="Total lines including comments and blank lines",
        ),
        "complexity": MetricDefinition(
            key="complexity",
            name="Cyclomatic Complexity",
            type=MetricType.COMPLEXITY,
            level=MetricLevel.PROJECT,
            unit="complexity",
            description="Cyclomatic complexity of the code",
        ),
        "cognitive_complexity": MetricDefinition(
            key="cognitive_complexity",
            name="Cognitive Complexity",
            type=MetricType.COMPLEXITY,
            level=MetricLevel.PROJECT,
            unit="complexity",
            description="Cognitive complexity of the code",
        ),
        "ncloc_language_distribution": MetricDefinition(
            key="ncloc_language_distribution",
            name="Language Distribution",
            type=MetricType.SIZE,
            level=MetricLevel.PROJECT,
            unit="distribution",
            description="Distribution of code across programming languages",
            numeric=False,
        ),
    }

    # Coverage metrics
    COVERAGE_METRICS: dict[str, MetricDefinition] = {
        "line_coverage": MetricDefinition(
            key="line_coverage",
            name="Line Coverage",
            type=MetricType.COVERAGE,
            level=MetricLevel.PROJECT,
            unit="%",
            description="Percentage of lines covered by tests",
            higher_is_better=True,
        ),
        "branch_coverage": MetricDefinition(
            key="branch_coverage",
            name="Branch Coverage",
            type=MetricType.COVERAGE,
            level=MetricLevel.PROJECT,
            unit="%",
            description="Percentage of branches covered by tests",
            higher_is_better=True,
        ),
        "test_success_density": MetricDefinition(
            key="test_success_density",
            name="Test Success Density",
            type=MetricType.COVERAGE,
            level=MetricLevel.PROJECT,
            unit="%",
            description="Percentage of successful tests",
            higher_is_better=True,
        ),
    }

    @classmethod
    def get_all_metrics(cls) -> dict[str, MetricDefinition]:
        """Get all registered metrics."""
        all_metrics = {}
        all_metrics.update(cls.CORE_QUALITY_METRICS)
        all_metrics.update(cls.SECURITY_METRICS)
        all_metrics.update(cls.RELIABILITY_METRICS)
        all_metrics.update(cls.MAINTAINABILITY_METRICS)
        all_metrics.update(cls.SIZE_COMPLEXITY_METRICS)
        all_metrics.update(cls.COVERAGE_METRICS)
        return all_metrics

    @classmethod
    def get_metric_definition(cls, metric_key: str) -> MetricDefinition:
        """Get metric definition by key.

        Args:
            metric_key: SonarQube metric key

        Returns:
            MetricDefinition if found, otherwise a generic definition
        """
        all_metrics = cls.get_all_metrics()

        if metric_key in all_metrics:
            return all_metrics[metric_key]

        # Return generic definition for unknown metrics
        return MetricDefinition(
            key=metric_key,
            name=metric_key.replace("_", " ").title(),
            type=MetricType.QUALITY,
            level=MetricLevel.PROJECT,
            unit="",
            description=f"Custom metric: {metric_key}",
        )

    @classmethod
    def get_metrics_by_type(cls, metric_type: MetricType) -> dict[str, MetricDefinition]:
        """Get all metrics of a specific type."""
        all_metrics = cls.get_all_metrics()
        return {key: definition for key, definition in all_metrics.items() if definition.type == metric_type}

    @classmethod
    def get_default_quality_metrics(cls) -> list[str]:
        """Get the default set of quality metrics used in SonarQube commands.

        Returns:
            List of metric keys for comprehensive quality analysis
        """
        return [
            "alert_status",
            "bugs",
            "reliability_rating",
            "vulnerabilities",
            "security_rating",
            "security_hotspots_reviewed",
            "security_review_rating",
            "code_smells",
            "sqale_rating",
            "duplicated_lines_density",
            "coverage",
            "ncloc",
            "ncloc_language_distribution",
            "security_issues",
            "reliability_issues",
            "maintainability_issues",
        ]

    @classmethod
    def get_essential_metrics(cls) -> list[str]:
        """Get essential metrics for quick analysis.

        Returns:
            List of most important metric keys
        """
        return [
            "alert_status",
            "bugs",
            "vulnerabilities",
            "code_smells",
            "coverage",
            "ncloc",
        ]

    @classmethod
    def validate_metric_keys(cls, metric_keys: list[str]) -> dict[str, bool]:
        """Validate if metric keys are known.

        Args:
            metric_keys: List of metric keys to validate

        Returns:
            Dictionary mapping metric key to validation status
        """
        all_metrics = cls.get_all_metrics()
        return {key: key in all_metrics for key in metric_keys}

    @classmethod
    def get_numeric_metrics(cls) -> set[str]:
        """Get set of numeric metric keys."""
        all_metrics = cls.get_all_metrics()
        return {key for key, definition in all_metrics.items() if definition.numeric}

    @classmethod
    def get_rating_metrics(cls) -> set[str]:
        """Get set of rating metric keys (A-E scale)."""
        all_metrics = cls.get_all_metrics()
        return {key for key, definition in all_metrics.items() if definition.unit == "rating"}

    @classmethod
    def get_percentage_metrics(cls) -> set[str]:
        """Get set of percentage metric keys."""
        all_metrics = cls.get_all_metrics()
        return {key for key, definition in all_metrics.items() if definition.unit == "%"}
