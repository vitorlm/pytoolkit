"""LinearB API Client for performance metrics and team analytics."""

import os
from datetime import datetime, timedelta
from typing import Any

import requests  # type: ignore

from utils.cache_manager.cache_manager import CacheManager
from utils.logging.logging_manager import LogManager


class LinearBApiClient:
    """Enhanced LinearB API client for retrieving performance metrics and team data."""

    def __init__(self):
        """Initialize the LinearB API client."""
        self.logger = LogManager.get_instance().get_logger("LinearBApiClient")
        self.cache = CacheManager.get_instance()
        self.base_url = os.getenv("LINEARB_BASE_URL", "https://app.linearb.io")
        self.api_key = os.getenv("LINEARB_API_KEY")

        if not self.api_key:
            raise ValueError("LINEARB_API_KEY environment variable is required")

        self.headers = {"x-api-key": self.api_key, "Content-Type": "application/json"}

        # Initialize helper classes
        self.metrics_manager = LinearBMetricsManager()
        self.time_helper = LinearBTimeRangeHelper()

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: dict | None = None,
        cache_key: str | None = None,
        cache_expiration: int = 60,
    ) -> dict[str, Any]:
        """Make a request to the LinearB API with enhanced caching and error handling.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            data: Request payload for POST requests
            cache_key: Cache key for storing results
            cache_expiration: Cache expiration in minutes

        Returns:
            API response data

        Raises:
            ValueError: For invalid parameters or API configuration
            requests.RequestException: For network/API errors
        """
        # Check cache first if cache_key is provided
        if cache_key:
            cached_data = self.cache.load(cache_key, expiration_minutes=cache_expiration)
            if cached_data is not None:
                self.logger.info(f"Using cached data for {cache_key}")
                return cached_data

        url = f"{self.base_url}{endpoint}"

        try:
            self.logger.info(f"Making {method} request to {endpoint}")

            # Add timeout to prevent hanging
            timeout = 30  # 30 seconds timeout

            if method.upper() == "GET":
                response = requests.get(url, headers=self.headers, timeout=timeout)
            elif method.upper() == "POST":
                response = requests.post(url, headers=self.headers, json=data, timeout=timeout)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            # Enhanced error handling based on LinearB API behavior
            if response.status_code == 202:
                # Export request accepted but not ready yet
                self.logger.info("Export request accepted, processing in background")
            elif response.status_code == 204:
                # No content - filters resulted in no data
                self.logger.warning("API returned no content - filters may have resulted in no data")
                return {
                    "detail": "No data available for the specified filters",
                    "data": [],
                }

            response.raise_for_status()
            result = response.json()

            # Cache the result if cache_key is provided
            if cache_key:
                self.cache.save(cache_key, result)
                self.logger.info(f"Cached result for {cache_key}")

            return result

        except requests.exceptions.Timeout as e:
            self.logger.error(f"Request timed out after 30 seconds: {e}")
            raise
        except requests.exceptions.RequestException as e:
            error_details = self._format_api_error(e, endpoint)
            self.logger.error(f"API request failed: {e}. {error_details}")
            raise

    def _format_api_error(self, error: requests.exceptions.RequestException, endpoint: str) -> str:
        """Format API error with LinearB-specific context."""
        if not hasattr(error, "response") or error.response is None:
            return str(error)

        status_code = error.response.status_code

        if status_code == 400:
            return (
                "400 Bad Request: Invalid request parameters. "
                "Check your filters, time ranges, and metric specifications."
            )
        elif status_code == 401:
            return "401 Unauthorized: Invalid API key or authentication failed."
        elif status_code == 403:
            return (
                f"403 Forbidden: The API key may be invalid or expired, "
                f"or the account may not have access to endpoint {endpoint}. "
                f"Please verify your LINEARB_API_KEY and account permissions."
            )
        elif status_code == 404:
            return f"404 Not Found: The endpoint {endpoint} was not found."
        elif status_code == 422:
            try:
                error_detail = error.response.json()
                return f"422 Validation Error: {error_detail}"
            except (ValueError, KeyError):
                return "422 Validation Error: Request validation failed."
        elif status_code == 500:
            return "500 Internal Server Error: LinearB server error. Please try again later."
        elif status_code == 504:
            return "504 Gateway Timeout: Request took too long to process. Try reducing the data range."
        else:
            return f"HTTP {status_code}: {error.response.text}"

    def test_connection(self) -> dict[str, Any]:
        """Test the API connection and validate the API key.

        Returns:
            Dictionary with connection test results.
        """
        try:
            self.logger.info("Testing LinearB API connection...")

            # Try a simple API call that should work with any valid API key
            _ = self.get_teams(page_size=1)

            self.logger.info("API connection test successful")
            return {
                "status": "success",
                "message": "API connection and key validation successful",
                "api_key_prefix": f"{self.api_key[:8]}..." if self.api_key else "None",
            }

        except Exception as e:
            self.logger.error(f"API connection test failed: {e}")
            return {
                "status": "failed",
                "message": str(e),
                "api_key_prefix": f"{self.api_key[:8]}..." if self.api_key else "None",
            }

    def get_teams(self, search_term: str | None = None, page_size: int = 50, offset: int = 0) -> dict[str, Any]:
        """Get teams from LinearB.

        Args:
            search_term: Optional search term to filter teams
            page_size: Number of teams per page (max 50)
            offset: Offset for pagination

        Returns:
            Teams data
        """
        endpoint = "/api/v2/teams"
        params: dict[str, Any] = {"page_size": min(page_size, 50), "offset": offset}

        if search_term:
            params["search_term"] = search_term

        # Add params to URL for GET request
        if params:
            param_str = "&".join([f"{k}={v}" for k, v in params.items()])
            endpoint += f"?{param_str}"

        cache_key = f"linearb_teams_{hash(str(params))}"
        return self._make_request("GET", endpoint, cache_key=cache_key, cache_expiration=120)

    def get_metrics(
        self,
        requested_metrics: list[dict[str, Any]],
        time_ranges: list[dict[str, str]],
        group_by: str = "team",
        team_ids: list[int] | None = None,
        contributor_ids: list[int] | None = None,
        repository_ids: list[int] | None = None,
        roll_up: str = "custom",
    ) -> dict[str, Any]:
        """Get performance metrics from LinearB.

        Args:
            requested_metrics: List of metrics to fetch
            time_ranges: List of time ranges to query
            group_by: Group by field (organization, team, contributor, repository, label)
            team_ids: Optional list of team IDs to filter by
            contributor_ids: Optional list of contributor IDs to filter by
            repository_ids: Optional list of repository IDs to filter by
            roll_up: Roll up period (1d, 1w, 1mo, custom)

        Returns:
            Metrics data
        """
        endpoint = "/api/v2/measurements"

        payload = {
            "group_by": group_by,
            "requested_metrics": requested_metrics,
            "time_ranges": time_ranges,
            "roll_up": roll_up,
        }

        if team_ids:
            payload["team_ids"] = [str(tid) for tid in team_ids]
        if contributor_ids:
            payload["contributor_ids"] = [str(cid) for cid in contributor_ids]
        if repository_ids:
            payload["repository_ids"] = [str(rid) for rid in repository_ids]

        cache_key = f"linearb_metrics_{hash(str(payload))}"
        return self._make_request("POST", endpoint, data=payload, cache_key=cache_key, cache_expiration=60)

    def export_metrics(
        self,
        requested_metrics: list[dict[str, Any]],
        time_ranges: list[dict[str, str]],
        group_by: str = "team",
        team_ids: list[int] | None = None,
        file_format: str = "json",
        roll_up: str = "custom",
        beautified: bool = False,
        return_no_data: bool = True,
        contributor_ids: list[int] | None = None,
        repository_ids: list[int] | None = None,
        service_ids: list[int] | None = None,
        labels: list[str] | None = None,
        limit: int | None = None,
        offset: int = 0,
        order_by: str | None = None,
        order_dir: str = "asc",
    ) -> dict[str, Any]:
        """Export performance metrics from LinearB using v2 API with dashboard-compatible parameters.

        Args:
            requested_metrics: List of metrics to fetch
            time_ranges: List of time ranges to query
            group_by: Group by field (organization, team, contributor, repository, label)
            team_ids: Optional list of team IDs to filter by
            file_format: Export format (csv, json)
            roll_up: Roll up period (1d, 1w, 1mo, custom)
            beautified: Format data in a more readable format (for CSV)
            return_no_data: Return contributors/teams with no data
            contributor_ids: Optional list of contributor IDs to filter by
            repository_ids: Optional list of repository IDs to filter by
            service_ids: Optional list of service IDs to filter by
            labels: Optional list of labels to filter/group by
            limit: Max amount of objects in response
            offset: Pagination offset
            order_by: Field name to order by
            order_dir: Ordering direction (asc, desc)

        Returns:
            Export response with report URL
        """
        endpoint = f"/api/v2/measurements/export?file_format={file_format}"

        payload = {
            "group_by": group_by,
            "requested_metrics": requested_metrics,
            "time_ranges": time_ranges,
            "roll_up": roll_up,
            "beautified": beautified,
            "return_no_data": return_no_data,
            "offset": offset,
            "order_dir": order_dir,
        }

        # Add optional parameters if provided
        if team_ids:
            payload["team_ids"] = team_ids
        if contributor_ids:
            payload["contributor_ids"] = contributor_ids
        if repository_ids:
            payload["repository_ids"] = repository_ids
        if service_ids:
            payload["service_ids"] = service_ids
        if labels:
            payload["labels"] = labels
        if limit:
            payload["limit"] = limit
        if order_by:
            payload["order_by"] = order_by

        return self._make_request("POST", endpoint, data=payload)

    # High-level convenience methods
    def get_engineering_metrics(
        self,
        time_period: str,
        team_ids: list[int] | None = None,
        aggregation: str = "default",
        group_by: str = "team",
        roll_up: str = "custom",
    ) -> dict[str, Any]:
        """Get engineering metrics with simplified interface.

        Args:
            time_period: Time period string (e.g., 'last-week', '30-days')
            team_ids: Optional list of team IDs to filter by
            aggregation: Aggregation type (default, p75, p50, avg)
            group_by: Group by field (organization, team, contributor, repository)
            roll_up: Roll up period (1d, 1w, 1mo, custom)

        Returns:
            Engineering metrics data
        """
        time_ranges = self.time_helper.parse_time_period(time_period)
        metrics = self.metrics_manager.get_default_engineering_metrics(aggregation)

        return self.get_metrics(
            requested_metrics=metrics,
            time_ranges=time_ranges,
            group_by=group_by,
            team_ids=team_ids,
            roll_up=roll_up,
        )

    def get_performance_metrics(
        self,
        time_period: str,
        team_ids: list[int] | None = None,
        aggregation: str = "default",
        group_by: str = "team",
        roll_up: str = "custom",
    ) -> dict[str, Any]:
        """Get performance metrics with simplified interface.

        Args:
            time_period: Time period string (e.g., 'last-week', '30-days')
            team_ids: Optional list of team IDs to filter by
            aggregation: Aggregation type (default, p75, p50, avg)
            group_by: Group by field (organization, team, contributor, repository)
            roll_up: Roll up period (1d, 1w, 1mo, custom)

        Returns:
            Performance metrics data
        """
        time_ranges = self.time_helper.parse_time_period(time_period)
        metrics = self.metrics_manager.get_default_performance_metrics(aggregation)

        return self.get_metrics(
            requested_metrics=metrics,
            time_ranges=time_ranges,
            group_by=group_by,
            team_ids=team_ids,
            roll_up=roll_up,
        )

    def get_knowledge_sharing_metrics(
        self,
        time_period: str,
        team_ids: list[int] | None = None,
        aggregation: str = "default",
        group_by: str = "team",
        roll_up: str = "custom",
    ) -> dict[str, Any]:
        """Get knowledge sharing metrics with simplified interface.

        Args:
            time_period: Time period string (e.g., 'last-week', '30-days')
            team_ids: Optional list of team IDs to filter by
            aggregation: Aggregation type (default, p75, p50, avg)
            group_by: Group by field (organization, team, contributor, repository)
            roll_up: Roll up period (1d, 1w, 1mo, custom)

        Returns:
            Knowledge sharing metrics data
        """
        time_ranges = self.time_helper.parse_time_period(time_period)
        metrics = self.metrics_manager.get_knowledge_sharing_metrics(aggregation)

        return self.get_metrics(
            requested_metrics=metrics,
            time_ranges=time_ranges,
            group_by=group_by,
            team_ids=team_ids,
            roll_up=roll_up,
        )

    def get_custom_metrics(
        self,
        metric_names: list[str],
        time_period: str,
        team_ids: list[int] | None = None,
        aggregation: str = "default",
        group_by: str = "team",
        roll_up: str = "custom",
    ) -> dict[str, Any]:
        """Get custom metrics with simplified interface.

        Args:
            metric_names: List of metric names to fetch
            time_period: Time period string (e.g., 'last-week', '30-days')
            team_ids: Optional list of team IDs to filter by
            aggregation: Aggregation type (default, p75, p50, avg)
            group_by: Group by field (organization, team, contributor, repository)
            roll_up: Roll up period (1d, 1w, 1mo, custom)

        Returns:
            Custom metrics data
        """
        time_ranges = self.time_helper.parse_time_period(time_period)
        metrics = self.metrics_manager.build_metrics_from_names(metric_names, aggregation)

        return self.get_metrics(
            requested_metrics=metrics,
            time_ranges=time_ranges,
            group_by=group_by,
            team_ids=team_ids,
            roll_up=roll_up,
        )

    def parse_team_ids(self, team_ids_input: Any) -> list[int] | None:
        """Parse team IDs from various input formats.

        Args:
            team_ids_input: Team IDs as string, list, or single integer

        Returns:
            List of team IDs or None if input is empty
        """
        if not team_ids_input:
            return None

        if isinstance(team_ids_input, str):
            return [int(tid.strip()) for tid in team_ids_input.split(",") if tid.strip()]
        elif isinstance(team_ids_input, list):
            return [int(tid) for tid in team_ids_input]
        elif isinstance(team_ids_input, int):
            return [team_ids_input]
        else:
            raise ValueError(f"Invalid team_ids format: {type(team_ids_input)}")

    def get_available_metrics(self) -> dict[str, Any]:
        """Get information about available metrics.

        Returns:
            Dictionary with metrics information
        """
        return {
            "metrics_with_aggregation": self.metrics_manager.get_metrics_requiring_aggregation(),
            "metrics_count_only": self.metrics_manager.get_metrics_count_only(),
            "default_engineering_metrics": [m["name"] for m in self.metrics_manager.get_default_engineering_metrics()],
            "default_performance_metrics": [m["name"] for m in self.metrics_manager.get_default_performance_metrics()],
            "knowledge_sharing_metrics": [m["name"] for m in self.metrics_manager.get_knowledge_sharing_metrics()],
            "aggregation_types": [
                LinearBAggregation.DEFAULT,
                LinearBAggregation.P75,
                LinearBAggregation.P50,
                LinearBAggregation.AVG,
                LinearBAggregation.RAW,
            ],
            "group_by_options": [
                LinearBGroupBy.ORGANIZATION,
                LinearBGroupBy.TEAM,
                LinearBGroupBy.CONTRIBUTOR,
                LinearBGroupBy.REPOSITORY,
                LinearBGroupBy.LABEL,
            ],
            "rollup_options": [
                LinearBRollup.DAILY,
                LinearBRollup.WEEKLY,
                LinearBRollup.MONTHLY,
                LinearBRollup.CUSTOM,
            ],
        }


class LinearBTimeRangeHelper:
    """Enhanced helper class for handling time ranges in LinearB format."""

    @staticmethod
    def parse_time_period(time_period: str) -> list[dict[str, str]]:
        """Parse time period string and return LinearB time ranges.

        Args:
            time_period: Time period string (e.g., 'last-week', 'last-2-weeks',
                        '30-days', 'YYYY-MM-DD,YYYY-MM-DD')

        Returns:
            List of time range dictionaries with 'after' and 'before' keys
        """
        today = datetime.now()

        if time_period == "last-week":
            start_date = today - timedelta(days=7)
            return [
                {
                    "after": start_date.strftime("%Y-%m-%d"),
                    "before": today.strftime("%Y-%m-%d"),
                }
            ]

        elif time_period == "last-2-weeks":
            start_date = today - timedelta(days=14)
            return [
                {
                    "after": start_date.strftime("%Y-%m-%d"),
                    "before": today.strftime("%Y-%m-%d"),
                }
            ]

        elif time_period == "last-month":
            start_date = today - timedelta(days=30)
            return [
                {
                    "after": start_date.strftime("%Y-%m-%d"),
                    "before": today.strftime("%Y-%m-%d"),
                }
            ]

        elif time_period.endswith("-days"):
            try:
                days = int(time_period.split("-")[0])
                start_date = today - timedelta(days=days)
                return [
                    {
                        "after": start_date.strftime("%Y-%m-%d"),
                        "before": today.strftime("%Y-%m-%d"),
                    }
                ]
            except ValueError:
                raise ValueError(f"Invalid time period format: {time_period}")

        elif "," in time_period:
            # Date range format: YYYY-MM-DD,YYYY-MM-DD
            try:
                start_str, end_str = time_period.split(",")
                # Validate date format
                datetime.strptime(start_str.strip(), "%Y-%m-%d")
                datetime.strptime(end_str.strip(), "%Y-%m-%d")
                return [{"after": start_str.strip(), "before": end_str.strip()}]
            except ValueError:
                raise ValueError(f"Invalid date range format: {time_period}. Expected YYYY-MM-DD,YYYY-MM-DD")

        else:
            raise ValueError(f"Unsupported time period format: {time_period}")

    @staticmethod
    def create_time_range(after: str, before: str) -> dict[str, str]:
        """Create a single time range dictionary.

        Args:
            after: Start date in YYYY-MM-DD format
            before: End date in YYYY-MM-DD format

        Returns:
            Time range dictionary
        """
        # Validate date formats
        try:
            datetime.strptime(after, "%Y-%m-%d")
            datetime.strptime(before, "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"Invalid date format. Expected YYYY-MM-DD: {e}")

        return {"after": after, "before": before}

    @staticmethod
    def get_relative_time_range(days_ago: int, days_duration: int = 1) -> list[dict[str, str]]:
        """Get time range relative to today.

        Args:
            days_ago: How many days ago to start
            days_duration: Duration in days (default: 1)

        Returns:
            List with single time range dictionary
        """
        today = datetime.now()
        start_date = today - timedelta(days=days_ago + days_duration - 1)
        end_date = today - timedelta(days=days_ago)

        return [
            {
                "after": start_date.strftime("%Y-%m-%d"),
                "before": end_date.strftime("%Y-%m-%d"),
            }
        ]


# Metrics Management Classes
class LinearBMetricsManager:
    """Centralized manager for LinearB metrics configuration and building."""

    # Time-based metrics
    TIME_TO_PR = "branch.time_to_pr"
    TIME_TO_APPROVE = "branch.time_to_approve"
    TIME_TO_MERGE = "branch.time_to_merge"
    TIME_TO_REVIEW = "branch.time_to_review"
    REVIEW_TIME = "branch.review_time"
    TIME_TO_PROD = "branch.time_to_prod"
    CYCLE_TIME = "branch.computed.cycle_time"

    # PR metrics
    PR_MERGED = "pr.merged"
    PR_MERGED_SIZE = "pr.merged.size"
    PR_NEW = "pr.new"
    PR_REVIEW_DEPTH = "pr.review_depth"
    PR_REVIEWS = "pr.reviews"
    PR_MERGED_WITHOUT_REVIEW = "pr.merged.without.review.count"
    PR_REVIEWED = "pr.reviewed"
    PR_MATURITY = "pr.maturity_ratio"

    # Commit metrics
    COMMIT_TOTAL_COUNT = "commit.total.count"
    COMMIT_NEW_WORK = "commit.activity.new_work.count"
    COMMIT_REWORK = "commit.activity.rework.count"
    COMMIT_REFACTOR = "commit.activity.refactor.count"
    COMMIT_TOTAL_CHANGES = "commit.total_changes"
    COMMIT_ACTIVITY_DAYS = "commit.activity_days"
    COMMIT_INVOLVED_REPOS = "commit.involved.repos.count"

    # Release metrics
    RELEASES_COUNT = "releases.count"
    DEPLOY_FREQUENCY = "releases.count"  # Alias for deploy frequency
    DEPLOY_TIME = "branch.time_to_prod"  # Alias for time to production

    # Other metrics
    BRANCH_STATE_DONE = "branch.state.computed.done"
    BRANCH_STATE_ACTIVE = "branch.state.active"
    PR_SIZE = "pr.merged.size"  # Alias for PR size
    PICKUP_TIME = "branch.time_to_review"  # Alias for pickup time
    CONTRIBUTOR_CODING_DAYS = "contributor.coding_days"

    # Branch commit coauthor (from OpenAPI schema)
    BRANCH_COMMIT_COAUTHOR = "branch.commit.coauthor"

    # Incident metrics (from OpenAPI schema)
    PM_MTTR = "pm.mttr"
    PM_CFR_ISSUES_DONE = "pm.cfr.issues.done"

    # GitStream metrics (from OpenAPI schema)
    GITSTREAM_SUGGESTION_COMMITS = "commit.total_count.gitstream.suggestion"
    GITSTREAM_SUGGESTION_CHANGES = "commit.total_changes.gitstream.suggestion"
    GITSTREAM_AI_REVIEW_TOTAL = "gitstream.ai.review.total.count"
    GITSTREAM_AI_REVIEW_PR = "gitstream.ai.review.pr.count"
    GITSTREAM_AI_SECURITY_PRS = "gitstream.ai.review.security_issues.prs.count"
    GITSTREAM_AI_BUGS_PRS = "gitstream.ai.review.bugs.prs.count"
    GITSTREAM_AI_PERFORMANCE_PRS = "gitstream.ai.review.performance_issues.prs.count"
    GITSTREAM_AI_READABILITY_PRS = "gitstream.ai.review.readability_issues.prs.count"
    GITSTREAM_AI_MAINTAINABILITY_PRS = "gitstream.ai.review.maintainability_issues.prs.count"

    # Code churn metrics
    COMMIT_CODE_CHURN_REWORK = "commit.code_churn.rework"
    COMMIT_CODE_CHURN_REFACTOR = "commit.code_churn.refactor"

    @classmethod
    def get_default_engineering_metrics(cls, aggregation: str = "default") -> list[dict[str, str]]:
        """Get default engineering metrics set for software development analysis."""
        return [
            {"name": cls.PR_MERGED_WITHOUT_REVIEW, "agg": "default"},
            {"name": cls.PR_REVIEW_DEPTH, "agg": aggregation},
            {"name": cls.PR_MATURITY, "agg": aggregation},
            {"name": cls.TIME_TO_PROD, "agg": aggregation},
            {"name": cls.PR_MERGED_SIZE, "agg": aggregation},
            {"name": cls.RELEASES_COUNT, "agg": "default"},
            {"name": cls.CYCLE_TIME, "agg": aggregation},
            {"name": cls.TIME_TO_REVIEW, "agg": aggregation},
            {"name": cls.REVIEW_TIME, "agg": aggregation},
        ]

    @classmethod
    def get_default_performance_metrics(cls, aggregation: str = "default") -> list[dict[str, str]]:
        """Get default performance metrics set for team performance analysis."""
        return [
            {"name": cls.CYCLE_TIME, "agg": aggregation},
            {"name": cls.TIME_TO_PR, "agg": aggregation},
            {"name": cls.TIME_TO_REVIEW, "agg": aggregation},
            {"name": cls.TIME_TO_MERGE, "agg": aggregation},
            {"name": cls.PR_MERGED, "agg": "default"},
            {"name": cls.PR_NEW, "agg": "default"},
            {"name": cls.COMMIT_TOTAL_COUNT, "agg": "default"},
            {"name": cls.RELEASES_COUNT, "agg": "default"},
        ]

    @classmethod
    def get_knowledge_sharing_metrics(cls, aggregation: str = "default") -> list[dict[str, str]]:
        """Get knowledge sharing metrics for collaboration analysis."""
        return [
            {"name": cls.PR_REVIEWS, "agg": "default"},
            {"name": cls.PR_REVIEW_DEPTH, "agg": aggregation},
            {"name": cls.PR_REVIEWED, "agg": "default"},
            {"name": cls.COMMIT_INVOLVED_REPOS, "agg": "default"},
            {"name": cls.BRANCH_COMMIT_COAUTHOR, "agg": "default"},
            {"name": cls.CONTRIBUTOR_CODING_DAYS, "agg": "default"},
        ]

    @classmethod
    def build_metrics_from_names(cls, metric_names: list[str], aggregation: str = "default") -> list[dict[str, str]]:
        """Build metrics list from metric names with specified aggregation."""
        return [{"name": name, "agg": aggregation} for name in metric_names]

    @classmethod
    def get_metrics_requiring_aggregation(cls) -> list[str]:
        """Get list of metrics that support aggregation (p75, p50, avg)."""
        return [
            cls.CYCLE_TIME,
            cls.TIME_TO_PR,
            cls.TIME_TO_APPROVE,
            cls.TIME_TO_MERGE,
            cls.TIME_TO_REVIEW,
            cls.REVIEW_TIME,
            cls.TIME_TO_PROD,
            cls.PR_MERGED_SIZE,
            cls.PR_REVIEW_DEPTH,
            cls.PR_MATURITY,
            cls.PM_MTTR,
        ]

    @classmethod
    def get_metrics_count_only(cls) -> list[str]:
        """Get list of metrics that only support count (no aggregation)."""
        return [
            cls.PR_MERGED,
            cls.PR_NEW,
            cls.PR_REVIEWS,
            cls.PR_MERGED_WITHOUT_REVIEW,
            cls.PR_REVIEWED,
            cls.COMMIT_TOTAL_COUNT,
            cls.COMMIT_NEW_WORK,
            cls.COMMIT_REWORK,
            cls.COMMIT_REFACTOR,
            cls.COMMIT_TOTAL_CHANGES,
            cls.COMMIT_ACTIVITY_DAYS,
            cls.COMMIT_INVOLVED_REPOS,
            cls.RELEASES_COUNT,
            cls.BRANCH_STATE_DONE,
            cls.BRANCH_STATE_ACTIVE,
            cls.CONTRIBUTOR_CODING_DAYS,
            cls.PM_CFR_ISSUES_DONE,
        ]


# Backward compatibility - Keep original class name as alias
class LinearBMetrics(LinearBMetricsManager):
    """Legacy alias for LinearBMetricsManager. Use LinearBMetricsManager for new code."""

    pass


class LinearBAggregation:
    """Constants for available aggregation types."""

    P75 = "p75"
    P50 = "p50"
    AVG = "avg"
    RAW = "raw"
    DEFAULT = "default"


class LinearBGroupBy:
    """Constants for available group by options."""

    ORGANIZATION = "organization"
    TEAM = "team"
    CONTRIBUTOR = "contributor"
    REPOSITORY = "repository"
    LABEL = "label"
    CUSTOM_METRIC = "custom_metric"


class LinearBRollup:
    """Constants for available rollup options."""

    DAILY = "1d"
    WEEKLY = "1w"
    MONTHLY = "1mo"
    CUSTOM = "custom"
