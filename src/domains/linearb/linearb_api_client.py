"""
LinearB API Client for performance metrics and team analytics.
"""

import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests  # type: ignore

from utils.cache_manager.cache_manager import CacheManager
from utils.logging.logging_manager import LogManager


class LinearBApiClient:
    """LinearB API client for retrieving performance metrics and team data."""

    def __init__(self):
        """Initialize the LinearB API client."""
        self.logger = LogManager.get_instance().get_logger("LinearBApiClient")
        self.cache = CacheManager.get_instance()
        self.base_url = os.getenv("LINEARB_BASE_URL", "https://app.linearb.io")
        self.api_key = os.getenv("LINEARB_API_KEY")

        if not self.api_key:
            raise ValueError("LINEARB_API_KEY environment variable is required")

        self.headers = {"x-api-key": self.api_key, "Content-Type": "application/json"}

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        cache_key: Optional[str] = None,
        cache_expiration: int = 60,
    ) -> Dict[str, Any]:
        """
        Make a request to the LinearB API with caching.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            data: Request payload for POST requests
            cache_key: Cache key for storing results
            cache_expiration: Cache expiration in minutes

        Returns:
            API response data
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

            if method.upper() == "GET":
                response = requests.get(url, headers=self.headers)
            elif method.upper() == "POST":
                response = requests.post(url, headers=self.headers, json=data)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()
            result = response.json()

            # Cache the result if cache_key is provided
            if cache_key:
                self.cache.save(cache_key, result)
                self.logger.info(f"Cached result for {cache_key}")

            return result

        except requests.exceptions.RequestException as e:
            error_details = ""
            if hasattr(e, "response") and e.response is not None:
                if e.response.status_code == 403:
                    error_details = (
                        f"403 Forbidden: The API key may be invalid or expired, "
                        f"or the account may not have access to endpoint {endpoint}. "
                        f"Please verify your LINEARB_API_KEY."
                    )
                elif e.response.status_code == 401:
                    error_details = "401 Unauthorized: Invalid API key or authentication failed."
                elif e.response.status_code == 404:
                    error_details = f"404 Not Found: The endpoint {endpoint} was not found."
                else:
                    error_details = f"HTTP {e.response.status_code}: {e.response.text}"

            self.logger.error(f"API request failed: {e}. {error_details}")
            raise

    def test_connection(self) -> Dict[str, Any]:
        """
        Test the API connection and validate the API key.

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

    def get_teams(
        self, search_term: Optional[str] = None, page_size: int = 50, offset: int = 0
    ) -> Dict[str, Any]:
        """
        Get teams from LinearB.

        Args:
            search_term: Optional search term to filter teams
            page_size: Number of teams per page (max 50)
            offset: Offset for pagination

        Returns:
            Teams data
        """
        endpoint = "/api/v2/teams"
        params: Dict[str, Any] = {"page_size": min(page_size, 50), "offset": offset}

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
        requested_metrics: List[Dict[str, Any]],
        time_ranges: List[Dict[str, str]],
        group_by: str = "team",
        team_ids: Optional[List[int]] = None,
        contributor_ids: Optional[List[int]] = None,
        repository_ids: Optional[List[int]] = None,
        roll_up: str = "custom",
    ) -> Dict[str, Any]:
        """
        Get performance metrics from LinearB.

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
        return self._make_request(
            "POST", endpoint, data=payload, cache_key=cache_key, cache_expiration=60
        )

    def export_metrics(
        self,
        requested_metrics: List[Dict[str, Any]],
        time_ranges: List[Dict[str, str]],
        group_by: str = "team",
        team_ids: Optional[List[int]] = None,
        file_format: str = "json",
        roll_up: str = "custom",
    ) -> Dict[str, Any]:
        """
        Export performance metrics from LinearB.

        Args:
            requested_metrics: List of metrics to fetch
            time_ranges: List of time ranges to query
            group_by: Group by field
            team_ids: Optional list of team IDs to filter by
            file_format: Export format (csv, json)
            roll_up: Roll up period

        Returns:
            Export response with report URL
        """
        endpoint = f"/api/v2/measurements/export?file_format={file_format}"

        payload = {
            "group_by": group_by,
            "requested_metrics": requested_metrics,
            "time_ranges": time_ranges,
            "roll_up": roll_up,
        }

        if team_ids:
            payload["team_ids"] = [str(tid) for tid in team_ids]

        return self._make_request("POST", endpoint, data=payload)


class LinearBTimeRangeHelper:
    """Helper class for handling time ranges in LinearB format."""

    @staticmethod
    def parse_time_period(time_period: str) -> List[Dict[str, str]]:
        """
        Parse time period string and return LinearB time ranges.

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
                raise ValueError(
                    f"Invalid date range format: {time_period}. Expected YYYY-MM-DD,YYYY-MM-DD"
                )

        else:
            raise ValueError(f"Unsupported time period format: {time_period}")


# Available metrics constants
class LinearBMetrics:
    """Constants for available LinearB metrics."""

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
