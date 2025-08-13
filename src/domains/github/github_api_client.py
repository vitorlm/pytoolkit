"""
GitHub API Client for PR analysis with rate limiting and caching.
"""

import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests

from utils.cache_manager.cache_manager import CacheManager
from utils.logging.logging_manager import LogManager


class GitHubApiClient:
    """GitHub REST API client with rate limiting and caching."""

    def __init__(self, max_workers: int = 4):
        self.logger = LogManager.get_instance().get_logger("GitHubApiClient")
        self.cache = CacheManager.get_instance()
        self.max_workers = max_workers

        self.base_url = os.getenv("GITHUB_BASE_URL", "https://api.github.com")
        self.api_version = os.getenv("GITHUB_API_VERSION", "2022-11-28")

        self.token = os.getenv("GITHUB_TOKEN")
        if not self.token:
            raise ValueError("GitHub token is required. Set GITHUB_TOKEN environment variable.")

        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": self.api_version,
            "User-Agent": "PyToolkit-PR-Analyzer/1.0",
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        cache_key: Optional[str] = None,
        cache_expiration: int = 60,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Make a request to the GitHub API with caching and retry logic.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            cache_key: Cache key for storing results
            cache_expiration: Cache expiration in minutes
            **kwargs: Additional request parameters

        Returns:
            API response data
        """
        # Try cache first for GET requests
        if method == "GET" and cache_key:
            cached_data = self.cache.load(cache_key, expiration_minutes=cache_expiration)
            if cached_data is not None:
                self.logger.debug(f"Using cached data for {endpoint}")
                return cached_data

        url = urljoin(self.base_url, endpoint.lstrip("/"))
        max_retries = 5
        base_delay = 1

        for attempt in range(max_retries):
            try:
                self.logger.debug(f"Making {method} request to {endpoint}")
                response = requests.request(method, url, headers=self.headers, **kwargs)

                # Log rate limit info
                remaining = response.headers.get("X-RateLimit-Remaining")
                reset_time = response.headers.get("X-RateLimit-Reset")
                if remaining and reset_time:
                    self.logger.debug(f"Rate limit remaining: {remaining}, resets at: {reset_time}")

                if response.status_code == 200:
                    data = response.json()

                    # Cache successful GET responses
                    if method == "GET" and cache_key:
                        self.cache.save(cache_key, data)

                    return data

                elif response.status_code == 404:
                    self.logger.warning(f"Resource not found: {endpoint}")
                    return {}

                elif response.status_code == 403:
                    if "rate limit exceeded" in response.text.lower():
                        reset_timestamp = int(reset_time) if reset_time else time.time() + 3600
                        wait_time = max(1, reset_timestamp - int(time.time()) + 10)
                        self.logger.warning(f"Rate limit exceeded. Waiting {wait_time} seconds...")
                        time.sleep(wait_time)
                        continue
                    else:
                        self.logger.error(f"Forbidden access to {endpoint}: {response.text}")
                        return {}

                elif response.status_code == 401:
                    self.logger.error(f"Unauthorized access to {endpoint}. Check your GitHub token.")
                    raise ValueError("Invalid GitHub token")

                elif response.status_code in [429, 500, 502, 503, 504]:
                    delay = base_delay * (2**attempt) + random.uniform(0, 1)
                    self.logger.warning(f"Request failed with {response.status_code}. Retrying in {delay:.2f}s...")
                    time.sleep(delay)
                    continue

                else:
                    self.logger.error(f"Unexpected status code {response.status_code} for {endpoint}")
                    return {}

            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    self.logger.error(f"Request failed after {max_retries} attempts: {e}")
                    raise
                delay = base_delay * (2**attempt) + random.uniform(0, 1)
                self.logger.warning(f"Request exception: {e}. Retrying in {delay:.2f}s...")
                time.sleep(delay)

        raise Exception(f"Failed to complete request to {endpoint} after {max_retries} attempts")

    def get_paginated_data(
        self,
        endpoint: str,
        per_page: int = 100,
        max_pages: Optional[int] = None,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """Get all pages of results from a paginated endpoint."""
        all_data = []
        page = 1
        pages_fetched = 0

        params = kwargs.get("params", {})

        while True:
            if max_pages and pages_fetched >= max_pages:
                break

            params.update({"per_page": per_page, "page": page})
            cache_key = f"{endpoint.replace('/', '_')}_page_{page}_{per_page}"

            try:
                data = self._make_request(
                    "GET",
                    endpoint,
                    cache_key=cache_key,
                    cache_expiration=30,  # 30 minutes for paginated data
                    params=params,
                )

                if not data or (isinstance(data, list) and len(data) == 0):
                    break

                if isinstance(data, list):
                    all_data.extend(data)
                else:
                    all_data.append(data)

                pages_fetched += 1
                self.logger.debug(f"Fetched page {page} from {endpoint}, got {len(data)} items")

                # If we got less than per_page items, this is probably the last page
                if isinstance(data, list) and len(data) < per_page:
                    break

                page += 1

            except Exception as e:
                self.logger.error(f"Failed to fetch page {page} from {endpoint}: {e}")
                break

        return all_data

    def get_team_members(self, org: str, team_slug: str) -> List[str]:
        """Get all members of a GitHub team."""
        self.logger.info(f"Fetching team members for {org}/{team_slug}")

        try:
            members_data = self.get_paginated_data(f"/orgs/{org}/teams/{team_slug}/members")
            members = [member["login"] for member in members_data if "login" in member]

            self.logger.info(f"Found {len(members)} members in team {org}/{team_slug}")
            return members

        except Exception as e:
            self.logger.error(f"Failed to fetch team members for {org}/{team_slug}: {e}")
            return []

    def get_org_repos(self, org: str, include_archived: bool = False) -> List[Dict[str, Any]]:
        """Get all repositories in an organization."""
        self.logger.info(f"Fetching repositories for organization {org}")

        try:
            repos = self.get_paginated_data(f"/orgs/{org}/repos", params={"type": "all"})

            if not include_archived:
                repos = [repo for repo in repos if not repo.get("archived", False)]

            self.logger.info(f"Found {len(repos)} repositories in {org}")
            return repos

        except Exception as e:
            self.logger.error(f"Failed to fetch repositories for {org}: {e}")
            return []

    def get_pull_requests(self, owner: str, repo: str, state: str = "all") -> List[Dict[str, Any]]:
        """Get pull requests for a repository."""
        self.logger.info(f"Fetching pull requests for {owner}/{repo} (state: {state})")

        try:
            prs = self.get_paginated_data(f"/repos/{owner}/{repo}/pulls", params={"state": state})

            self.logger.info(f"Found {len(prs)} pull requests in {owner}/{repo}")
            return prs

        except Exception as e:
            self.logger.error(f"Failed to fetch pull requests for {owner}/{repo}: {e}")
            return []

    def resolve_team_members_parallel(self, team_specs: List[str]) -> set:
        """Resolve team members from multiple teams in parallel."""
        self.logger.info(f"Resolving members for {len(team_specs)} teams")
        all_members: set[str] = set()

        # Parse team specifications
        teams_to_resolve = []
        for team_spec in team_specs:
            if not team_spec.startswith("@"):
                self.logger.warning(f"Invalid team format: {team_spec} (should start with @)")
                continue

            try:
                parts = team_spec[1:].split("/", 1)
                if len(parts) != 2:
                    self.logger.warning(f"Invalid team format: {team_spec} (should be @org/team)")
                    continue

                org, team_slug = parts
                teams_to_resolve.append((org, team_slug))
            except Exception as e:
                self.logger.error(f"Failed to parse team spec {team_spec}: {e}")
                continue

        if not teams_to_resolve:
            self.logger.warning("No valid teams to resolve")
            return all_members

        # Resolve teams in parallel
        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(teams_to_resolve))) as executor:
            future_to_team = {
                executor.submit(self.get_team_members, org, team_slug): f"{org}/{team_slug}"
                for org, team_slug in teams_to_resolve
            }

            for future in as_completed(future_to_team):
                team_name = future_to_team[future]
                try:
                    members = future.result()
                    all_members.update(members)
                    self.logger.debug(f"Added {len(members)} members from {team_name}")
                except Exception as e:
                    self.logger.error(f"Failed to resolve team {team_name}: {e}")

        self.logger.info(f"Total unique team members resolved: {len(all_members)}")
        return all_members

    def get_pull_request_details(self, owner: str, repo: str, pr_number: int) -> Dict[str, Any]:
        """Get detailed information for a specific pull request."""
        self.logger.debug(f"Fetching PR details for {owner}/{repo}#{pr_number}")

        try:
            pr_details = self._make_request(
                "GET",
                f"/repos/{owner}/{repo}/pulls/{pr_number}",
                cache_key=f"pr_details_{owner}_{repo}_{pr_number}",
                cache_expiration=15,  # 15 minutes cache for PR details
            )

            if pr_details:
                self.logger.debug(f"Retrieved PR details for {owner}/{repo}#{pr_number}")
            else:
                self.logger.warning(f"No PR details found for {owner}/{repo}#{pr_number}")

            return pr_details

        except Exception as e:
            self.logger.error(f"Failed to fetch PR details for {owner}/{repo}#{pr_number}: {e}")
            return {}

    def get_pull_request_reviews(self, owner: str, repo: str, pr_number: int) -> List[Dict[str, Any]]:
        """Get all reviews for a pull request."""
        self.logger.debug(f"Fetching PR reviews for {owner}/{repo}#{pr_number}")

        try:
            reviews = self.get_paginated_data(f"/repos/{owner}/{repo}/pulls/{pr_number}/reviews")
            self.logger.debug(f"Found {len(reviews)} reviews for {owner}/{repo}#{pr_number}")
            return reviews

        except Exception as e:
            self.logger.error(f"Failed to fetch PR reviews for {owner}/{repo}#{pr_number}: {e}")
            return []

    def get_pull_request_comments(self, owner: str, repo: str, pr_number: int) -> List[Dict[str, Any]]:
        """Get all review comments (code comments) for a pull request."""
        self.logger.debug(f"Fetching PR review comments for {owner}/{repo}#{pr_number}")

        try:
            comments = self.get_paginated_data(f"/repos/{owner}/{repo}/pulls/{pr_number}/comments")
            self.logger.debug(f"Found {len(comments)} review comments for {owner}/{repo}#{pr_number}")
            return comments

        except Exception as e:
            self.logger.error(f"Failed to fetch PR review comments for {owner}/{repo}#{pr_number}: {e}")
            return []

    def get_issue_comments(self, owner: str, repo: str, issue_number: int) -> List[Dict[str, Any]]:
        """Get all issue comments for a pull request (PRs are issues too)."""
        self.logger.debug(f"Fetching issue comments for {owner}/{repo}#{issue_number}")

        try:
            comments = self.get_paginated_data(f"/repos/{owner}/{repo}/issues/{issue_number}/comments")
            self.logger.debug(f"Found {len(comments)} issue comments for {owner}/{repo}#{issue_number}")
            return comments

        except Exception as e:
            self.logger.error(f"Failed to fetch issue comments for {owner}/{repo}#{issue_number}: {e}")
            return []

    def enrich_pull_request(
        self,
        owner: str,
        repo: str,
        pr_data: Dict[str, Any],
        include_size_metrics: bool = True,
        include_review_metrics: bool = True,
    ) -> Dict[str, Any]:
        """
        Enrich a pull request with additional metadata using parallel API calls.

        Args:
            owner: Repository owner
            repo: Repository name
            pr_data: Basic PR data from pulls API
            include_size_metrics: Whether to include size metrics
            include_review_metrics: Whether to include review/discussion metrics

        Returns:
            Enriched PR data dictionary
        """
        pr_number: int = pr_data.get("number", 0)
        if not pr_number:
            self.logger.error("PR number is missing or invalid")
            return pr_data

        enriched_data = pr_data.copy()

        try:
            if include_size_metrics or include_review_metrics:
                # Use parallel execution for multiple API calls
                with ThreadPoolExecutor(max_workers=min(4, self.max_workers)) as executor:
                    future_tasks: Dict[str, Any] = {}

                    # Always get PR details if we need any enrichment
                    future_tasks["pr_details"] = executor.submit(self.get_pull_request_details, owner, repo, pr_number)

                    # Only fetch review data if review metrics are needed
                    if include_review_metrics:
                        future_tasks["reviews"] = executor.submit(self.get_pull_request_reviews, owner, repo, pr_number)
                        future_tasks["review_comments"] = executor.submit(
                            self.get_pull_request_comments, owner, repo, pr_number
                        )
                        future_tasks["issue_comments"] = executor.submit(
                            self.get_issue_comments, owner, repo, pr_number
                        )

                    # Collect results with proper typing
                    results: Dict[str, Any] = {}
                    for task_name, future in future_tasks.items():
                        try:
                            results[task_name] = future.result(timeout=30)  # 30 second timeout per API call
                        except Exception as e:
                            self.logger.warning(f"Failed to get {task_name} for PR {pr_number}: {e}")
                            results[task_name] = [] if task_name != "pr_details" else {}

                pr_details = results.get("pr_details", {})

                # Add size metrics
                if include_size_metrics and pr_details:
                    enriched_data.update(
                        {
                            "additions": pr_details.get("additions", 0),
                            "deletions": pr_details.get("deletions", 0),
                            "changed_files": pr_details.get("changed_files", 0),
                            "commits": pr_details.get("commits", 0),
                        }
                    )
                else:
                    enriched_data.update(
                        {
                            "additions": None,
                            "deletions": None,
                            "changed_files": None,
                            "commits": None,
                        }
                    )

                # Add review metrics
                if include_review_metrics:
                    reviews: List[Dict[str, Any]] = results.get("reviews", [])
                    review_comments: List[Dict[str, Any]] = results.get("review_comments", [])
                    issue_comments: List[Dict[str, Any]] = results.get("issue_comments", [])

                    # Extract reviewer data from PR details
                    requested_reviewers = pr_details.get("requested_reviewers", []) if pr_details else []
                    requested_teams = pr_details.get("requested_teams", []) if pr_details else []

                    # Calculate metrics
                    approvals_count = sum(
                        1 for review in reviews if isinstance(review, dict) and review.get("state") == "APPROVED"
                    )

                    # Calculate time to first review and first response
                    time_to_first_review_seconds = None
                    first_response_latency_seconds = None

                    created_at = self._parse_timestamp(pr_data.get("created_at", ""))

                    if created_at:
                        response_times = []

                        # Find first review time
                        if reviews:
                            review_times = [
                                self._parse_timestamp(review.get("submitted_at", ""))
                                for review in reviews
                                if isinstance(review, dict) and review.get("submitted_at")
                            ]
                            # Filter out None values
                            valid_review_times = [t for t in review_times if t is not None]
                            if valid_review_times:
                                first_review_time = min(valid_review_times)
                                time_to_first_review_seconds = int((first_review_time - created_at).total_seconds())
                                response_times.append(first_review_time)

                        # Find first comment time
                        if issue_comments:
                            comment_times = [
                                self._parse_timestamp(comment.get("created_at", ""))
                                for comment in issue_comments
                                if isinstance(comment, dict) and comment.get("created_at")
                            ]
                            # Filter out None values
                            valid_comment_times = [t for t in comment_times if t is not None]
                            if valid_comment_times:
                                response_times.append(min(valid_comment_times))

                        # Calculate overall first response latency
                        if response_times:
                            first_response_time = min(response_times)
                            first_response_latency_seconds = int((first_response_time - created_at).total_seconds())

                    enriched_data.update(
                        {
                            "requested_reviewers": [r.get("login") for r in requested_reviewers],
                            "requested_teams": [t.get("slug") for t in requested_teams],
                            "requested_reviewers_count": len(requested_reviewers) + len(requested_teams),
                            "reviews_count": len(reviews),
                            "approvals_count": approvals_count,
                            "review_comments": len(review_comments),
                            "issue_comments": len(issue_comments),
                            "time_to_first_review_seconds": time_to_first_review_seconds,
                            "first_response_latency_seconds": first_response_latency_seconds,
                        }
                    )
                else:
                    enriched_data.update(
                        {
                            "requested_reviewers": None,
                            "requested_teams": None,
                            "requested_reviewers_count": None,
                            "reviews_count": None,
                            "approvals_count": None,
                            "review_comments": None,
                            "issue_comments": None,
                            "time_to_first_review_seconds": None,
                            "first_response_latency_seconds": None,
                        }
                    )

            else:
                # Add null values for all enrichment fields when disabled
                enriched_data.update(
                    {
                        "additions": None,
                        "deletions": None,
                        "changed_files": None,
                        "commits": None,
                        "requested_reviewers": None,
                        "requested_teams": None,
                        "requested_reviewers_count": None,
                        "reviews_count": None,
                        "approvals_count": None,
                        "review_comments": None,
                        "issue_comments": None,
                        "time_to_first_review_seconds": None,
                        "first_response_latency_seconds": None,
                    }
                )

            return enriched_data

        except Exception as e:
            self.logger.error(f"Failed to enrich PR {owner}/{repo}#{pr_number}: {e}")
            return enriched_data

    def _parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """Parse ISO 8601 timestamp string to datetime object."""
        if not timestamp_str:
            return None
        try:
            return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        except Exception:
            return None
