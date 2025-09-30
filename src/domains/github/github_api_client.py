"""
GitHub API Client for PR analysis with rate limiting and caching.
"""

import os
import random
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from utils.cache_manager.cache_manager import CacheManager
from utils.logging.logging_manager import LogManager

# Allow override via environment variable
GRAPHQL_ENDPOINT = os.getenv(
    "GITHUB_GRAPHQL_ENDPOINT", "https://api.github.com/graphql"
)


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
            raise ValueError(
                "GitHub token is required. Set GITHUB_TOKEN environment variable."
            )

        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": self.api_version,
            "User-Agent": "PyToolkit-PR-Analyzer/1.0",
        }

        # Separate headers for GraphQL (needs JSON accept)
        self.graphql_headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "PyToolkit-PR-Analyzer/1.0",
        }

        # Debug: mostrar caminho do módulo carregado (para evitar confusão de múltiplas cópias)
        try:
            self.logger.debug(f"GitHubApiClient loaded from: {__file__}")
        except Exception:
            pass

    # ----------------------------- GraphQL Support -----------------------------
    def post_graphql(
        self,
        query: str,
        variables: Dict[str, Any],
        max_retries: int = 5,
        backoff_base: float = 1.0,
    ) -> Dict[str, Any]:
        """Execute a GraphQL POST request with retry/backoff.

        Args:
            query: GraphQL query string
            variables: Variables dict
            max_retries: Maximum attempts
            backoff_base: Base seconds for exponential backoff

        Returns:
            Parsed JSON response ("data" object)
        """
        url = GRAPHQL_ENDPOINT
        attempt = 0
        while attempt < max_retries:
            try:
                resp = requests.post(
                    url,
                    headers=self.graphql_headers,
                    data=json.dumps({"query": query, "variables": variables}),
                    timeout=30,
                )
                if resp.status_code == 200:
                    payload = resp.json()
                    if "errors" in payload:
                        # Always surface full error list for visibility
                        self.logger.warning(
                            f"GraphQL errors (attempt {attempt + 1}/{max_retries}): "
                            + "; ".join(
                                str(e.get("message")) for e in payload.get("errors", [])
                            )
                        )
                        # Detect transient classes we should retry
                        joined = " ".join(
                            e.get("message", "").lower()
                            for e in payload.get("errors", [])
                        )
                        if any(
                            k in joined
                            for k in [
                                "rate limit",
                                "secondary rate limit",
                                "timeout",
                                "timed out",
                            ]
                        ):
                            raise RuntimeError("Transient GraphQL error")
                    # Return full payload (data + optionally errors) so caller can inspect repository presence
                    return payload
                elif resp.status_code in (502, 503, 504, 429):
                    self.logger.warning(
                        f"GraphQL transient status {resp.status_code} attempt {attempt + 1}/{max_retries}"
                    )
                elif resp.status_code == 401:
                    raise ValueError("Unauthorized GraphQL request - invalid token")
                else:
                    self.logger.error(
                        f"GraphQL unexpected status {resp.status_code}: {resp.text[:200]}"
                    )
                    # Non-retryable
                    break
            except Exception as e:
                if attempt == max_retries - 1:
                    self.logger.error(f"GraphQL request failed permanently: {e}")
                    raise
                delay = backoff_base * (2**attempt) + random.uniform(0, 0.25)
                self.logger.debug(f"GraphQL retry in {delay:.2f}s after error: {e}")
                time.sleep(delay)
            attempt += 1
        return {}

    def _make_request(
        self,
        method: str,
        endpoint: str,
        cache_key: Optional[str] = None,
        cache_expiration: int = 60,
        **kwargs,
    ) -> Any:
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
            cached_data = self.cache.load(
                cache_key, expiration_minutes=cache_expiration
            )
            if cached_data is not None:
                self.logger.debug(f"Using cached data for {endpoint}")
                return cached_data

        url = urljoin(self.base_url, endpoint.lstrip("/"))
        max_retries = 5
        base_delay = 1

        for attempt in range(max_retries):
            try:
                self.logger.info(f"Making {method} request to {endpoint}")
                response = requests.request(method, url, headers=self.headers, **kwargs)

                # Log rate limit info
                remaining = response.headers.get("X-RateLimit-Remaining")
                reset_time = response.headers.get("X-RateLimit-Reset")
                if remaining and reset_time:
                    self.logger.debug(
                        f"Rate limit remaining: {remaining}, resets at: {reset_time}"
                    )

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
                        reset_timestamp = (
                            int(reset_time) if reset_time else time.time() + 3600
                        )
                        wait_time = max(1, reset_timestamp - int(time.time()) + 10)
                        self.logger.warning(
                            f"Rate limit exceeded. Waiting {wait_time} seconds..."
                        )
                        time.sleep(wait_time)
                        continue
                    else:
                        self.logger.error(
                            f"Forbidden access to {endpoint}: {response.text}"
                        )
                        return {}

                elif response.status_code == 401:
                    self.logger.error(
                        f"Unauthorized access to {endpoint}. Check your GitHub token."
                    )
                    raise ValueError("Invalid GitHub token")

                elif response.status_code in [429, 500, 502, 503, 504]:
                    delay = base_delay * (2**attempt) + random.uniform(0, 1)
                    self.logger.warning(
                        f"Request failed with {response.status_code}. Retrying in {delay:.2f}s..."
                    )
                    time.sleep(delay)
                    continue

                else:
                    self.logger.error(
                        f"Unexpected status code {response.status_code} for {endpoint}"
                    )
                    return {}

            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    self.logger.error(
                        f"Request failed after {max_retries} attempts: {e}"
                    )
                    raise
                delay = base_delay * (2**attempt) + random.uniform(0, 1)
                self.logger.warning(
                    f"Request exception: {e}. Retrying in {delay:.2f}s..."
                )
                time.sleep(delay)

        raise Exception(
            f"Failed to complete request to {endpoint} after {max_retries} attempts"
        )

    def get_paginated_data(
        self,
        endpoint: str,
        per_page: int = 100,
        max_pages: Optional[int] = None,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """Get all pages of results from a paginated endpoint."""
        all_data: List[Dict[str, Any]] = []
        page = 1
        pages_fetched = 0

        params = kwargs.get("params", {})

        while True:
            if max_pages and pages_fetched >= max_pages:
                break

            params.update({"per_page": per_page, "page": page})
            cache_key = f"{endpoint.replace('/', '_')}_page_{page}_{per_page}"

            try:
                # Better progress logging for longer operations
                if page == 1:
                    self.logger.info(f"📡 Starting to fetch data from {endpoint}")
                elif page % 5 == 0:  # Log every 5 pages
                    self.logger.info(
                        f"📡 Fetching page {page} from {endpoint} (total items so far: {len(all_data)})"
                    )

                data = self._make_request(
                    "GET",
                    endpoint,
                    cache_key=cache_key,
                    cache_expiration=30,  # 30 minutes for paginated data
                    params=params,
                )

                if not data or (isinstance(data, list) and len(data) == 0):
                    self.logger.info(
                        f"📡 Finished fetching from {endpoint} - no more data on page {page}"
                    )
                    break

                if isinstance(data, list):
                    all_data.extend(data)  # type: ignore
                else:
                    all_data.append(data)

                pages_fetched += 1

                # More informative progress for first few pages and larger datasets
                if (
                    page <= 3
                    or len(all_data) % 200 == 0
                    or (isinstance(data, list) and len(data) < per_page)
                ):
                    self.logger.info(
                        f"📡 Page {page} from {endpoint}: +{len(data)} items (total: {len(all_data)})"
                    )

                # If we got less than per_page items, this is probably the last page
                if isinstance(data, list) and len(data) < per_page:
                    self.logger.info(
                        f"📡 Completed fetching from {endpoint}: {len(all_data)} total items across {pages_fetched} pages"
                    )
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
            members_data = self.get_paginated_data(
                f"/orgs/{org}/teams/{team_slug}/members"
            )
            members = [member["login"] for member in members_data if "login" in member]

            self.logger.info(f"Found {len(members)} members in team {org}/{team_slug}")
            return members

        except Exception as e:
            self.logger.error(
                f"Failed to fetch team members for {org}/{team_slug}: {e}"
            )
            return []

    def get_org_repos(
        self, org: str, include_archived: bool = False
    ) -> List[Dict[str, Any]]:
        """Get all repositories in an organization."""
        self.logger.info(f"Fetching repositories for organization {org}")

        try:
            repos = self.get_paginated_data(
                f"/orgs/{org}/repos", params={"type": "all"}
            )

            if not include_archived:
                repos = [repo for repo in repos if not repo.get("archived", False)]

            self.logger.info(f"Found {len(repos)} repositories in {org}")
            return repos

        except Exception as e:
            self.logger.error(f"Failed to fetch repositories for {org}: {e}")
            return []

    def get_pull_requests(
        self, owner: str, repo: str, state: str = "all"
    ) -> List[Dict[str, Any]]:
        """Get pull requests for a repository."""
        self.logger.info(f"Fetching pull requests for {owner}/{repo} (state: {state})")

        try:
            prs = self.get_paginated_data(
                f"/repos/{owner}/{repo}/pulls", params={"state": state}
            )

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
                self.logger.warning(
                    f"Invalid team format: {team_spec} (should start with @)"
                )
                continue

            try:
                parts = team_spec[1:].split("/", 1)
                if len(parts) != 2:
                    self.logger.warning(
                        f"Invalid team format: {team_spec} (should be @org/team)"
                    )
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
        with ThreadPoolExecutor(
            max_workers=min(self.max_workers, len(teams_to_resolve))
        ) as executor:
            future_to_team = {
                executor.submit(
                    self.get_team_members, org, team_slug
                ): f"{org}/{team_slug}"
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

    def get_pull_request_details(
        self, owner: str, repo: str, pr_number: int
    ) -> Dict[str, Any]:
        """Get detailed information for a specific pull request."""
        self.logger.info(f"🔍 Fetching PR details for {owner}/{repo}#{pr_number}")

        try:
            pr_details = self._make_request(
                "GET",
                f"/repos/{owner}/{repo}/pulls/{pr_number}",
                cache_key=f"pr_details_{owner}_{repo}_{pr_number}",
                cache_expiration=15,  # 15 minutes cache for PR details
            )

            if pr_details:
                self.logger.info(
                    f"✅ Retrieved PR details for {owner}/{repo}#{pr_number}"
                )
            else:
                self.logger.warning(
                    f"⚠️ No PR details found for {owner}/{repo}#{pr_number}"
                )

            return pr_details

        except Exception as e:
            self.logger.error(
                f"❌ Failed to fetch PR details for {owner}/{repo}#{pr_number}: {e}"
            )
            return {}

    def get_pull_request_reviews(
        self, owner: str, repo: str, pr_number: int
    ) -> List[Dict[str, Any]]:
        """Get all reviews for a pull request."""
        self.logger.debug(f"Fetching PR reviews for {owner}/{repo}#{pr_number}")

        try:
            reviews = self.get_paginated_data(
                f"/repos/{owner}/{repo}/pulls/{pr_number}/reviews"
            )
            self.logger.debug(
                f"Found {len(reviews)} reviews for {owner}/{repo}#{pr_number}"
            )
            return reviews

        except Exception as e:
            self.logger.error(
                f"Failed to fetch PR reviews for {owner}/{repo}#{pr_number}: {e}"
            )
            return []

    def get_pull_request_comments(
        self, owner: str, repo: str, pr_number: int
    ) -> List[Dict[str, Any]]:
        """Get all review comments (code comments) for a pull request."""
        self.logger.debug(f"Fetching PR review comments for {owner}/{repo}#{pr_number}")

        try:
            comments = self.get_paginated_data(
                f"/repos/{owner}/{repo}/pulls/{pr_number}/comments"
            )
            self.logger.debug(
                f"Found {len(comments)} review comments for {owner}/{repo}#{pr_number}"
            )
            return comments

        except Exception as e:
            self.logger.error(
                f"Failed to fetch PR review comments for {owner}/{repo}#{pr_number}: {e}"
            )
            return []

    def get_issue_comments(
        self, owner: str, repo: str, issue_number: int
    ) -> List[Dict[str, Any]]:
        """Get all issue comments for a pull request (PRs are issues too)."""
        self.logger.debug(f"Fetching issue comments for {owner}/{repo}#{issue_number}")

        try:
            comments = self.get_paginated_data(
                f"/repos/{owner}/{repo}/issues/{issue_number}/comments"
            )
            self.logger.debug(
                f"Found {len(comments)} issue comments for {owner}/{repo}#{issue_number}"
            )
            return comments

        except Exception as e:
            self.logger.error(
                f"Failed to fetch issue comments for {owner}/{repo}#{issue_number}: {e}"
            )
            return []

    def enrich_pull_request(
        self,
        owner: str,
        repo: str,
        pr_data: Dict[str, Any],
        include_size_metrics: bool = True,
        include_review_metrics: bool = True,
        include_review_rounds: bool = False,
    ) -> Dict[str, Any]:
        """
        Enrich a pull request with additional metadata using parallel API calls.

        Args:
            owner: Repository owner
            repo: Repository name
            pr_data: Basic PR data from pulls API
            include_size_metrics: Whether to include size metrics
            include_review_metrics: Whether to include review/discussion metrics
            include_review_rounds: Whether to include review rounds calculation

        Returns:
            Enriched PR data dictionary
        """
        pr_number: int = pr_data.get("number", 0)
        if not pr_number:
            self.logger.error("PR number is missing or invalid")
            return pr_data

        enriched_data = pr_data.copy()

        try:
            if include_size_metrics or include_review_metrics or include_review_rounds:
                # Count API calls for progress tracking
                api_calls_needed = []
                if (
                    include_size_metrics
                    or include_review_metrics
                    or include_review_rounds
                ):
                    api_calls_needed.append("pr_details")
                if include_review_metrics or include_review_rounds:
                    api_calls_needed.extend(
                        ["reviews", "review_comments", "issue_comments"]
                    )
                if include_review_rounds:
                    api_calls_needed.extend(["timeline", "commits"])

                self.logger.info(
                    f"🔍 Enriching PR #{pr_number} with {len(api_calls_needed)} API calls: {', '.join(api_calls_needed)}"
                )

                # Use parallel execution for multiple API calls
                with ThreadPoolExecutor(
                    max_workers=min(6, self.max_workers)
                ) as executor:
                    future_tasks: Dict[str, Any] = {}

                    # Always get PR details if we need any enrichment
                    future_tasks["pr_details"] = executor.submit(
                        self.get_pull_request_details, owner, repo, pr_number
                    )

                    # Only fetch review data if review metrics are needed
                    if include_review_metrics or include_review_rounds:
                        future_tasks["reviews"] = executor.submit(
                            self.get_pull_request_reviews, owner, repo, pr_number
                        )
                        future_tasks["review_comments"] = executor.submit(
                            self.get_pull_request_comments, owner, repo, pr_number
                        )
                        future_tasks["issue_comments"] = executor.submit(
                            self.get_issue_comments, owner, repo, pr_number
                        )

                    # Fetch timeline and commits if review rounds are needed
                    if include_review_rounds:
                        future_tasks["timeline"] = executor.submit(
                            self.get_issue_timeline, owner, repo, pr_number
                        )
                        future_tasks["commits"] = executor.submit(
                            self.get_pull_request_commits, owner, repo, pr_number
                        )

                    # Collect results with proper typing
                    results: Dict[str, Any] = {}
                    for task_name, future in future_tasks.items():
                        try:
                            results[task_name] = future.result(
                                timeout=30
                            )  # 30 second timeout per API call
                        except Exception as e:
                            self.logger.warning(
                                f"Failed to get {task_name} for PR {pr_number}: {e}"
                            )
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
                if include_review_metrics or include_review_rounds:
                    reviews: List[Dict[str, Any]] = results.get("reviews", [])
                    review_comments: List[Dict[str, Any]] = results.get(
                        "review_comments", []
                    )
                    issue_comments: List[Dict[str, Any]] = results.get(
                        "issue_comments", []
                    )

                    # Extract reviewer data from PR details
                    requested_reviewers = (
                        pr_details.get("requested_reviewers", []) if pr_details else []
                    )
                    requested_teams = (
                        pr_details.get("requested_teams", []) if pr_details else []
                    )

                    # Calculate metrics
                    approvals_count = sum(
                        1
                        for review in reviews
                        if isinstance(review, dict)
                        and review.get("state") == "APPROVED"
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
                                if isinstance(review, dict)
                                and review.get("submitted_at")
                            ]
                            # Filter out None values
                            valid_review_times = [
                                t for t in review_times if t is not None
                            ]
                            if valid_review_times:
                                first_review_time = min(valid_review_times)
                                time_to_first_review_seconds = int(
                                    (first_review_time - created_at).total_seconds()
                                )
                                response_times.append(first_review_time)

                        # Find first comment time
                        if issue_comments:
                            comment_times = [
                                self._parse_timestamp(comment.get("created_at", ""))
                                for comment in issue_comments
                                if isinstance(comment, dict)
                                and comment.get("created_at")
                            ]
                            # Filter out None values
                            valid_comment_times = [
                                t for t in comment_times if t is not None
                            ]
                            if valid_comment_times:
                                response_times.append(min(valid_comment_times))

                        # Calculate overall first response latency
                        if response_times:
                            first_response_time = min(response_times)
                            first_response_latency_seconds = int(
                                (first_response_time - created_at).total_seconds()
                            )

                    if include_review_metrics:
                        enriched_data.update(
                            {
                                "requested_reviewers": [
                                    r.get("login") for r in requested_reviewers
                                ],
                                "requested_teams": [
                                    t.get("slug") for t in requested_teams
                                ],
                                "requested_reviewers_count": len(requested_reviewers)
                                + len(requested_teams),
                                "reviews_count": len(reviews),
                                "approvals_count": approvals_count,
                                "review_comments": len(review_comments),
                                "issue_comments": len(issue_comments),
                                "time_to_first_review_seconds": time_to_first_review_seconds,
                                "first_response_latency_seconds": first_response_latency_seconds,
                            }
                        )

                    # Calculate review rounds if requested
                    if include_review_rounds:
                        review_rounds_data = self._calculate_review_rounds(
                            reviews,
                            results.get("timeline", []),
                            results.get("commits", []),
                            created_at,
                        )
                        enriched_data.update(review_rounds_data)
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
                            "review_rounds": None,
                            "synchronize_after_first_review": None,
                            "re_review_pushes": None,
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
                        "review_rounds": None,
                        "synchronize_after_first_review": None,
                        "re_review_pushes": None,
                    }
                )

            return enriched_data

        except Exception as e:
            self.logger.error(f"Failed to enrich PR {owner}/{repo}#{pr_number}: {e}")
            return enriched_data

    def get_issue_timeline(
        self, owner: str, repo: str, issue_number: int
    ) -> List[Dict[str, Any]]:
        """Get timeline events for an issue/PR."""
        self.logger.debug(f"Fetching timeline for {owner}/{repo}#{issue_number}")

        try:
            timeline = self.get_paginated_data(
                f"/repos/{owner}/{repo}/issues/{issue_number}/timeline"
            )
            self.logger.debug(
                f"Found {len(timeline)} timeline events for {owner}/{repo}#{issue_number}"
            )
            return timeline

        except Exception as e:
            self.logger.error(
                f"Failed to fetch timeline for {owner}/{repo}#{issue_number}: {e}"
            )
            return []

    def get_pull_request_commits(
        self, owner: str, repo: str, pr_number: int
    ) -> List[Dict[str, Any]]:
        """Get all commits for a pull request."""
        self.logger.debug(f"Fetching commits for {owner}/{repo}#{pr_number}")

        try:
            commits = self.get_paginated_data(
                f"/repos/{owner}/{repo}/pulls/{pr_number}/commits"
            )
            self.logger.debug(
                f"Found {len(commits)} commits for {owner}/{repo}#{pr_number}"
            )
            return commits

        except Exception as e:
            self.logger.error(
                f"Failed to fetch commits for {owner}/{repo}#{pr_number}: {e}"
            )
            return []

    def _calculate_review_rounds(
        self,
        reviews: List[Dict[str, Any]],
        timeline: List[Dict[str, Any]],
        commits: List[Dict[str, Any]],
        created_at: Optional[datetime],
    ) -> Dict[str, Any]:
        """
        Calculate review rounds for a PR.

        A review round is counted for each sequence: review → synchronize → review

        Args:
            reviews: List of review submissions
            timeline: List of timeline events
            commits: List of commits for fallback calculation
            created_at: PR creation timestamp

        Returns:
            Dictionary with review_rounds, synchronize_after_first_review, re_review_pushes
        """
        review_rounds = 0
        synchronize_after_first_review = 0
        re_review_pushes = 0
        try:
            if not reviews or not created_at:
                return {
                    "review_rounds": review_rounds,
                    "synchronize_after_first_review": synchronize_after_first_review,
                    "re_review_pushes": re_review_pushes,
                }

            # Parse review timestamps
            review_events: List[tuple[str, datetime, Dict[str, Any]]] = []
            for review in reviews:
                submitted_at = review.get("submitted_at")
                if submitted_at:
                    ts = self._parse_timestamp(submitted_at)
                    if ts:
                        review_events.append(("review", ts, review))

            # Synchronize events (treat as commits/pushes)
            commit_events: List[tuple[str, datetime, Dict[str, Any]]] = []
            for event in timeline:
                if event.get("event") == "synchronize":
                    timestamp_str = event.get("created_at")
                    if timestamp_str:
                        ts = self._parse_timestamp(timestamp_str)
                        if ts:
                            commit_events.append(("commit", ts, event))

            # Fallback to commits list if timeline lacks synchronize events
            if not commit_events and commits:
                for commit in commits:
                    commit_date_str = (
                        commit.get("commit", {}).get("author", {}).get("date")
                    )
                    if commit_date_str:
                        c_ts = self._parse_timestamp(commit_date_str)
                        if c_ts:
                            commit_events.append(("commit", c_ts, commit))

            all_events = review_events + commit_events
            all_events.sort(key=lambda x: x[1])

            if not all_events:
                return {
                    "review_rounds": review_rounds,
                    "synchronize_after_first_review": synchronize_after_first_review,
                    "re_review_pushes": re_review_pushes,
                }

            # Find first review
            first_review_time = None
            for event_type, timestamp, _ in all_events:
                if event_type == "review":
                    first_review_time = timestamp
                    break

            if not first_review_time:
                return {
                    "review_rounds": review_rounds,
                    "synchronize_after_first_review": synchronize_after_first_review,
                    "re_review_pushes": re_review_pushes,
                }

            # Count review rounds: transitions from review → synchronize → review
            last_event_type = None
            in_review_round = False

            for event_type, timestamp, _ in all_events:
                if event_type == "review":
                    if last_event_type == "commit" and in_review_round:
                        review_rounds += 1
                        in_review_round = False
                    elif last_event_type != "review":
                        in_review_round = True
                elif event_type == "commit":
                    if timestamp > first_review_time:
                        synchronize_after_first_review += 1

                last_event_type = event_type

            # Fallback calculation using commits if timeline is unavailable
            if review_rounds == 0 and synchronize_after_first_review == 0 and commits:
                # Count commits pushed after first review
                for commit in commits:
                    commit_date_str = (
                        commit.get("commit", {}).get("author", {}).get("date")
                    )
                    if commit_date_str:
                        commit_date = self._parse_timestamp(commit_date_str)
                        if (
                            commit_date
                            and first_review_time
                            and commit_date > first_review_time
                        ):
                            re_review_pushes += 1

            return {
                "review_rounds": review_rounds,
                "synchronize_after_first_review": synchronize_after_first_review,
                "re_review_pushes": re_review_pushes,
            }

        except Exception as e:
            self.logger.warning(f"Failed to calculate review rounds: {e}")
            return {
                "review_rounds": review_rounds,
                "synchronize_after_first_review": synchronize_after_first_review,
                "re_review_pushes": re_review_pushes,
            }

    def _parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """Parse ISO 8601 timestamp string to datetime object."""
        if not timestamp_str:
            return None
        try:
            return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        except Exception:
            return None

    # ----------------------- GraphQL PR Fetch with Approvers (Enhanced) -----------------------
    def fetch_pull_requests_graphql_with_reviews(
        self,
        owner: str,
        repo: str,
        since_iso: Optional[str] = None,
        until_iso: Optional[str] = None,
        page_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Fetch PRs via GraphQL with comprehensive review and approval data.

        This method fetches all the approval and review data needed for the new fields:
        - approvers: List[str] - unique reviewer logins who have at least one APPROVED review
        - approvers_count: int - len(approvers)
        - latest_approvals: List[{"login": str, "submitted_at": str}] - last APPROVED per reviewer
        - review_decision: Optional[str] - PullRequest.reviewDecision
        - approvals_valid_now: Optional[int] - heuristic count of approvals considered valid
        - approvals_after_last_push: Optional[int] - approvals submitted after last push

        Args:
            owner: Repository owner
            repo: Repository name
            since_iso: ISO timestamp to filter PRs updated since this date
            page_size: Number of PRs to fetch per page

        Returns:
            List of enriched PR dictionaries with approval data
        """
        query = """
        query($owner: String!, $name: String!, $pageSize: Int!, $cursor: String) {
          rateLimit { 
            cost 
            remaining 
            resetAt 
            used 
          }
          repository(owner: $owner, name: $name) {
            pullRequests(
              first: $pageSize, 
              after: $cursor, 
              orderBy: {field: UPDATED_AT, direction: DESC}, 
              states: [OPEN, MERGED, CLOSED]
            ) {
              totalCount
              pageInfo {
                hasNextPage 
                endCursor
              }
              nodes {
                number
                url
                title
                state
                createdAt
                mergedAt
                updatedAt
                closedAt
                baseRefName
                headRefName
                author {
                  login
                }
                authorAssociation
                isDraft
                additions
                deletions
                changedFiles
                commits {
                  totalCount
                }
                comments {
                  totalCount
                }
                reviewThreads {
                  totalCount
                }
                reviewDecision
                reviews(first: 100) {
                  nodes {
                    state
                    submittedAt
                    author {
                      login
                    }
                  }
                }
                latestOpinionatedReviews(first: 100) {
                  nodes {
                    state
                    submittedAt
                    author {
                      login
                    }
                  }
                }
                reviewRequests(first: 100) {
                  nodes {
                    requestedReviewer {
                      __typename
                      ... on User {
                        login
                      }
                      ... on Team {
                        slug
                      }
                    }
                  }
                }
                timelineItems(first: 100, itemTypes: [PULL_REQUEST_COMMIT]) {
                  nodes {
                    __typename
                    ... on PullRequestCommit {
                      commit {
                        committedDate
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """

        variables: Dict[str, Any] = {
            "owner": owner,
            "name": repo,
            "pageSize": page_size,
            "cursor": None,
        }
        since_cutoff = self._parse_timestamp(since_iso) if since_iso else None
        until_cutoff = self._parse_timestamp(until_iso) if until_iso else None
        all_prs: List[Dict[str, Any]] = []
        page_index = 0

        self.logger.info(
            f"Fetching PRs with review data via GraphQL for {owner}/{repo}"
        )
        if since_cutoff or until_cutoff:
            self.logger.info(
                f"🗓️  Date filtering: since={since_cutoff}, until={until_cutoff}"
            )

        while True:
            raw = self.post_graphql(query, variables)
            data = raw.get("data", {}) if isinstance(raw, dict) else {}
            errors = raw.get("errors") if isinstance(raw, dict) else None

            # Handle errors and repository validation
            if page_index == 0:
                if errors:
                    messages = " | ".join(
                        str(e.get("message")) for e in errors if isinstance(e, dict)
                    )
                    self.logger.error(f"GraphQL errors: {messages}")
                    if "Could not resolve to a Repository" in messages:
                        self.logger.error(
                            f"GraphQL repo resolve failed {owner}/{repo}: {messages}"
                        )
                        break

            repo_data = data.get("repository") if isinstance(data, dict) else None
            if page_index == 0:
                if repo_data is None:
                    self.logger.error(
                        f"GraphQL: repository data is None for {owner}/{repo}"
                    )
                    break

            pr_connection = (repo_data or {}).get("pullRequests", {})
            nodes = (
                pr_connection.get("nodes", [])
                if isinstance(pr_connection, dict)
                else []
            )
            total_count = (
                pr_connection.get("totalCount")
                if isinstance(pr_connection, dict)
                else None
            )

            # Rate limit and progress logging (first page only)
            if page_index == 0:
                rl = data.get("rateLimit") if isinstance(data, dict) else None
                if isinstance(rl, dict):
                    rl_used = int(rl.get("used") or 0)
                    rl_remaining = int(rl.get("remaining") or 0)
                    self.logger.info(
                        f"📊 Rate Limit: {rl_used}/{rl_used + rl_remaining} used"
                    )

                if total_count:
                    self.logger.info(
                        f"🎯 Repository has {total_count} total PRs to process"
                    )

            if not nodes:
                if page_index == 0:
                    self.logger.warning(
                        f"GraphQL returned zero PR nodes for {owner}/{repo} (page_size={page_size}, since={since_iso})."
                    )
                    self.logger.warning(f"PR connection data: {pr_connection}")
                break

            self.logger.info(
                f"📄 Page {page_index + 1}: processing {len(nodes)} PR nodes"
            )

            batch: List[Dict[str, Any]] = []
            skipped_too_recent = 0
            since_cutoff_reached = False

            for node in nodes:
                updated_at = node.get("updatedAt")
                updated_dt = self._parse_timestamp(updated_at) if updated_at else None

                # Apply date filtering (PRs are ordered by updatedAt DESC - newest first)
                if since_cutoff and updated_dt and updated_dt < since_cutoff:
                    # PR updated before since date - stop processing entirely
                    self.logger.info(
                        f"⏰ Since cutoff reached at PR #{node.get('number')} (updated: {updated_at})"
                    )
                    since_cutoff_reached = True
                    break

                if until_cutoff and updated_dt and updated_dt > until_cutoff:
                    # PR updated after until date - skip this PR but continue (we haven't reached our date range yet)
                    skipped_too_recent += 1
                    continue

                # PR is within our date range (or no date filters applied)
                batch.append(self._map_graphql_pr_with_approvers(owner, repo, node))

            if skipped_too_recent > 0:
                self.logger.info(
                    f"⏭️  Skipped {skipped_too_recent} PRs that were too recent (after until date)"
                )

            if isinstance(batch, list):
                all_prs.extend(batch)
                self.logger.info(
                    f"📄 Page {page_index + 1}: processed {len(batch)} PRs (total so far: {len(all_prs)})"
                )

                # Show progress every 10 pages for large datasets
                if total_count and total_count > 100 and (page_index + 1) % 10 == 0:
                    progress_pct = (len(all_prs) / total_count) * 100
                    self.logger.info(
                        f"📈 Progress: {len(all_prs)}/{total_count} PRs ({progress_pct:.1f}%)"
                    )

            # Stop if since cutoff was reached
            if since_cutoff_reached:
                break

            page_info = (
                pr_connection.get("pageInfo", {})
                if isinstance(pr_connection, dict)
                else {}
            )
            has_next_page = page_info.get("hasNextPage", False)
            self.logger.info(
                f"📄 Pagination: hasNextPage={has_next_page}, cursor={page_info.get('endCursor', 'None')}"
            )

            if not has_next_page:
                self.logger.info("📄 No more pages available")
                break
            variables["cursor"] = page_info.get("endCursor")
            page_index += 1

        self.logger.info(
            f"✅ GraphQL with reviews completed: fetched {len(all_prs)} PRs for {owner}/{repo} in {page_index + 1} API calls"
        )

        return all_prs

    # ----------------------- GraphQL PR Fetch (Fast Path) -----------------------
    def fetch_pull_requests_graphql(
        self,
        owner: str,
        repo: str,
        since_iso: Optional[str],
        page_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """Fetch PRs via GraphQL (updatedAt DESC) with early cutoff.

        Returns REST-like dicts. Adds diagnostics if empty. Does NOT fallback automatically; caller can decide.
        """
        query = """
        query($owner: String!, $name: String!, $pageSize: Int!, $cursor: String) {
          rateLimit { 
            cost 
            remaining 
            resetAt 
            used 
          }
          repository(owner: $owner, name: $name) {
            pullRequests(
              first: $pageSize, 
              after: $cursor, 
              orderBy: {field: UPDATED_AT, direction: DESC}, 
              states: [OPEN, MERGED, CLOSED]
            ) {
              totalCount
              pageInfo {
                hasNextPage 
                endCursor
              }
              nodes {
                number
                url
                title
                state
                createdAt
                mergedAt
                updatedAt
                closedAt
                baseRefName
                headRefName
                author {
                  login
                }
                authorAssociation
                isDraft
                additions
                deletions
                changedFiles
                commits {
                  totalCount
                }
                comments {
                  totalCount
                }
                reviewThreads {
                  totalCount
                }
                reviews(first: 100) {
                  nodes {
                    state
                    submittedAt
                    author {
                      login
                    }
                  }
                }
                reviewRequests(first: 100) {
                  nodes {
                    requestedReviewer {
                      __typename
                      ... on User {
                        login
                      }
                      ... on Team {
                        slug
                      }
                    }
                  }
                }
                timelineItems(first: 100) {
                  nodes {
                    __typename
                    ... on PullRequestCommit {
                      commit {
                        committedDate
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """
        variables: Dict[str, Any] = {
            "owner": owner,
            "name": repo,
            "pageSize": page_size,
            "cursor": None,
        }
        cutoff = self._parse_timestamp(since_iso) if since_iso else None
        all_prs: List[Dict[str, Any]] = []
        page_index = 0

        while True:
            raw = self.post_graphql(query, variables)
            data = raw.get("data", {}) if isinstance(raw, dict) else {}
            errors = raw.get("errors") if isinstance(raw, dict) else None

            # More detailed error and data logging
            if page_index == 0:
                self.logger.info(
                    f"GraphQL response structure: data={type(data)}, errors={type(errors)}"
                )
                if isinstance(data, dict):
                    self.logger.info(f"GraphQL data keys: {list(data.keys())}")
                else:
                    self.logger.error(f"GraphQL data is not a dict: {data}")

                if errors:
                    messages = " | ".join(
                        str(e.get("message")) for e in errors if isinstance(e, dict)
                    )
                    self.logger.error(f"GraphQL errors: {messages}")
                    if "Could not resolve to a Repository" in messages:
                        self.logger.error(
                            f"GraphQL repo resolve failed {owner}/{repo}: {messages}"
                        )
                        break

                # Debug: log full raw response when debugging
                if os.getenv("GITHUB_GRAPHQL_DEBUG") == "1":
                    import json

                    self.logger.info(
                        f"RAW GraphQL response: {json.dumps(raw, indent=2)[:2000]}"
                    )

            repo_data = data.get("repository") if isinstance(data, dict) else None
            if page_index == 0:
                if repo_data is None:
                    self.logger.error(
                        f"GraphQL: repository data is None for {owner}/{repo}"
                    )
                    if isinstance(data, dict) and len(data) == 0:
                        self.logger.error(
                            "GraphQL returned empty data object - possible authentication or permission issue"
                        )
                    break
                else:
                    self.logger.info(f"GraphQL: repository found for {owner}/{repo}")

            pr_connection = (repo_data or {}).get("pullRequests", {})
            nodes = (
                pr_connection.get("nodes", [])
                if isinstance(pr_connection, dict)
                else []
            )
            total_count = (
                pr_connection.get("totalCount")
                if isinstance(pr_connection, dict)
                else None
            )

            if page_index == 0:
                rl = data.get("rateLimit") if isinstance(data, dict) else None
                if isinstance(rl, dict):
                    rl_used = int(rl.get("used") or 0)
                    rl_remaining = int(rl.get("remaining") or 0)
                    rl_cost = rl.get("cost")
                    self.logger.info(
                        f"📊 GraphQL Rate Limit: cost={rl_cost}, used={rl_used}/{rl_used + rl_remaining}, remaining={rl_remaining}, resets at {rl.get('resetAt')}"
                    )

                # Show total count from first page
                if repo_data and isinstance(repo_data, dict):
                    pr_connection = repo_data.get("pullRequests", {})
                    if isinstance(pr_connection, dict):
                        total_count = pr_connection.get("totalCount", 0)
                        self.logger.info(
                            f"🎯 Repository has {total_count} total PRs to process"
                        )

                # Debug logging
                if os.getenv("GITHUB_GRAPHQL_DEBUG") == "1":
                    try:
                        import json as _json

                        self.logger.info(f"GraphQL DEBUG: {_json.dumps(raw)[:900]}")
                    except Exception:
                        pass

            if not nodes:
                if page_index == 0:
                    self.logger.warning(
                        f"GraphQL returned zero PR nodes for {owner}/{repo} (page_size={page_size}, since={since_iso})."
                    )
                    if total_count:
                        self.logger.error(
                            f"GraphQL anomaly: totalCount={total_count} but nodes list empty. Raw payload will be logged."
                        )
                    try:
                        import json as _json

                        self.logger.info(
                            f"GraphQL RAW FIRST PAGE: {_json.dumps(raw)[:900]}"
                        )
                    except Exception:
                        pass
                    try:
                        import json as _json

                        self.logger.info(
                            f"GraphQL RAW FIRST PAGE: {_json.dumps(raw)[:900]}"
                        )
                    except Exception:
                        pass
                    try:
                        import json as _json

                        snippet = _json.dumps(raw)[:600]
                        self.logger.info(
                            f"GraphQL empty first page raw snippet: {snippet}"
                        )
                    except Exception:
                        pass
                break

            batch: List[Dict[str, Any]] = []
            for node in nodes:
                updated_at = node.get("updatedAt")
                updated_dt = self._parse_timestamp(updated_at) if updated_at else None
                if cutoff and updated_dt and updated_dt < cutoff:
                    # Early cutoff reached; stop completely
                    batch.append(self._map_graphql_pr(owner, repo, node))
                    self.logger.info(
                        f"⏰ Early cutoff reached at PR #{node.get('number')} (updated: {updated_at})"
                    )
                    break
                batch.append(self._map_graphql_pr(owner, repo, node))

            if isinstance(batch, list):
                all_prs.extend(batch)
                self.logger.info(
                    f"📄 Page {page_index + 1}: processed {len(batch)} PRs (total so far: {len(all_prs)})"
                )

                # Show progress for large datasets
                if total_count and total_count > 100:
                    progress_pct = (len(all_prs) / total_count) * 100
                    self.logger.info(
                        f"📈 Progress: {len(all_prs)}/{total_count} PRs ({progress_pct:.1f}%)"
                    )

            # Stop if cutoff triggered inside loop
            if cutoff and batch:
                last_updated = self._parse_timestamp(batch[-1].get("updated_at", ""))
                if last_updated and last_updated < cutoff:
                    break

            page_info = (
                pr_connection.get("pageInfo", {})
                if isinstance(pr_connection, dict)
                else {}
            )
            if not page_info.get("hasNextPage"):
                break
            variables["cursor"] = page_info.get("endCursor")
            page_index += 1

        self.logger.info(
            f"✅ GraphQL completed: fetched {len(all_prs)} PRs for {owner}/{repo} in {page_index + 1} API calls"
        )

        # Final rate limit check
        if page_index > 0:  # Don't repeat if only one page
            try:
                final_rl = (
                    self.post_graphql("query { rateLimit { remaining used } }", {})
                    .get("data", {})
                    .get("rateLimit", {})
                )
                if final_rl:
                    fr_used = int(final_rl.get("used") or 0)
                    fr_remaining = int(final_rl.get("remaining") or 0)
                    self.logger.info(
                        f"📊 Final Rate Limit: {fr_used}/{fr_used + fr_remaining} used"
                    )
            except Exception:
                pass

        return all_prs

    def _map_graphql_pr(
        self, owner: str, repo: str, node: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Map a GraphQL PR node to REST-like schema expected downstream."""
        number = node.get("number")
        created_at = node.get("createdAt")
        merged_at = node.get("mergedAt")
        closed_at = node.get("closedAt")  # May be present for closed (merged or not)
        state_raw = (node.get("state") or "").lower()
        state = "closed" if state_raw in {"merged", "closed"} else state_raw

        reviews_nodes = node.get("reviews", {}).get("nodes", [])
        approvals_count = sum(
            1 for r in reviews_nodes if (r.get("state") == "APPROVED")
        )
        review_times = [
            self._parse_timestamp(r.get("submittedAt"))
            for r in reviews_nodes
            if r.get("submittedAt")
        ]
        created_dt = self._parse_timestamp(created_at) if created_at else None
        time_to_first_review_seconds = None
        first_response_latency_seconds = None

        if created_dt:
            # Calculate time to first review
            if review_times:
                valid_review_times = [t for t in review_times if t]
                if valid_review_times:
                    first_review_time = min(valid_review_times)
                    time_to_first_review_seconds = int(
                        (first_review_time - created_dt).total_seconds()
                    )
                    first_response_latency_seconds = time_to_first_review_seconds

        requested_nodes = node.get("reviewRequests", {}).get("nodes", [])
        requested_reviewers: List[str] = []
        requested_teams: List[str] = []
        for n in requested_nodes:
            rr = n.get("requestedReviewer", {}) or {}
            typename = rr.get("__typename")
            if typename == "User":
                login_val = rr.get("login")
                if isinstance(login_val, str):
                    requested_reviewers.append(login_val)
            elif typename == "Team":
                slug_val = rr.get("slug")
                if isinstance(slug_val, str):
                    requested_teams.append(slug_val)

        commits_total = node.get("commits", {}).get("totalCount")

        # Calculate review rounds using timeline data (commit events)
        timeline_nodes = node.get("timelineItems", {}).get("nodes", [])
        review_rounds_data = self._calculate_review_rounds_from_timeline(
            reviews_nodes, timeline_nodes, created_dt
        )

        pr_dict: Dict[str, Any] = {
            "repo": f"{owner}/{repo}",
            "number": number,
            "html_url": node.get("url"),
            "title": node.get("title"),
            "state": state,
            "created_at": created_at,
            "merged_at": merged_at,
            # closedAt comes from GraphQL; if absent but mergedAt present treat closed_at as mergedAt
            "closed_at": closed_at or merged_at or None,
            "updated_at": node.get("updatedAt"),
            "user": {"login": (node.get("author") or {}).get("login")},
            "base": {"ref": node.get("baseRefName")},
            "head": {"ref": node.get("headRefName")},
            "additions": node.get("additions"),
            "deletions": node.get("deletions"),
            "changed_files": node.get("changedFiles"),
            "commits": commits_total,
            "reviews_count": len(reviews_nodes),
            "approvals_count": approvals_count,
            "review_comments": node.get("reviewThreads", {}).get("totalCount") or 0,
            "issue_comments": node.get("comments", {}).get("totalCount") or 0,
            "requested_reviewers": requested_reviewers,
            "requested_teams": requested_teams,
            "requested_reviewers_count": len(requested_reviewers)
            + len(requested_teams),
            "time_to_first_review_seconds": time_to_first_review_seconds,
            "first_response_latency_seconds": first_response_latency_seconds,
            "is_draft": node.get("isDraft"),
            "author_association": node.get("authorAssociation"),
            # Review rounds from GraphQL timeline data
            "review_rounds": review_rounds_data.get("review_rounds"),
            "synchronize_after_first_review": review_rounds_data.get(
                "synchronize_after_first_review"
            ),
            "re_review_pushes": review_rounds_data.get("re_review_pushes"),
            "source_api": "graphql",
        }
        return pr_dict

    def _calculate_review_rounds_from_timeline(
        self,
        reviews_nodes: List[Dict[str, Any]],
        timeline_nodes: List[Dict[str, Any]],
        created_at: Optional[datetime],
    ) -> Dict[str, Any]:
        """
        Calculate review rounds using GraphQL timeline data.

        Args:
            reviews_nodes: Review nodes from GraphQL
            timeline_nodes: Timeline nodes from GraphQL (synchronize events)
            created_at: PR creation timestamp

        Returns:
            Dictionary with review_rounds, synchronize_after_first_review, re_review_pushes
        """
        review_rounds = 0
        synchronize_after_first_review = 0
        re_review_pushes = 0

        try:
            if not reviews_nodes or not created_at:
                return {
                    "review_rounds": review_rounds,
                    "synchronize_after_first_review": synchronize_after_first_review,
                    "re_review_pushes": re_review_pushes,
                }

            # Parse and sort review events
            review_events = []
            for review in reviews_nodes:
                submitted_at = review.get("submittedAt")
                if submitted_at:
                    timestamp = self._parse_timestamp(submitted_at)
                    if timestamp:
                        review_events.append(("review", timestamp, review))

            # Parse commit events from timeline (PullRequestCommit)
            commit_events = []
            for event in timeline_nodes:
                if event.get("__typename") == "PullRequestCommit":
                    commit_data = event.get("commit", {})
                    timestamp_str = commit_data.get("committedDate")
                    if timestamp_str:
                        timestamp = self._parse_timestamp(timestamp_str)
                        if timestamp:
                            commit_events.append(("commit", timestamp, event))

            # Combine and sort all events chronologically
            all_events = review_events + commit_events
            all_events.sort(key=lambda x: x[1])  # Sort by timestamp

            if not all_events:
                return {
                    "review_rounds": review_rounds,
                    "synchronize_after_first_review": synchronize_after_first_review,
                    "re_review_pushes": re_review_pushes,
                }

            # Find first review
            first_review_time = None
            for event_type, timestamp, _ in all_events:
                if event_type == "review":
                    first_review_time = timestamp
                    break

            if not first_review_time:
                return {
                    "review_rounds": review_rounds,
                    "synchronize_after_first_review": synchronize_after_first_review,
                    "re_review_pushes": re_review_pushes,
                }

            # Count review rounds: transitions from review → synchronize → review
            last_event_type = None
            in_review_round = False

            for event_type, timestamp, _ in all_events:
                if event_type == "review":
                    if last_event_type == "commit" and in_review_round:
                        review_rounds += 1
                        in_review_round = False
                    elif last_event_type != "review":
                        in_review_round = True
                elif event_type == "commit":
                    if timestamp > first_review_time:
                        synchronize_after_first_review += 1

                last_event_type = event_type

            # re_review_pushes is essentially same as commits after first review
            re_review_pushes = synchronize_after_first_review

            return {
                "review_rounds": review_rounds,
                "synchronize_after_first_review": synchronize_after_first_review,
                "re_review_pushes": re_review_pushes,
            }

        except Exception as e:
            self.logger.warning(f"Failed to calculate review rounds from GraphQL: {e}")
            return {
                "review_rounds": review_rounds,
                "synchronize_after_first_review": synchronize_after_first_review,
                "re_review_pushes": re_review_pushes,
            }

    def _map_graphql_pr_with_approvers(
        self, owner: str, repo: str, node: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Map a GraphQL PR node to REST-like schema with enhanced approver data.

        This method extends the basic mapping to include:
        - approvers: List[str] - unique reviewer logins who have APPROVED reviews
        - approvers_count: int - number of unique approvers
        - latest_approvals: List[Dict] - latest APPROVED review per reviewer
        - review_decision: Optional[str] - GitHub's review decision
        - approvals_valid_now: Optional[int] - heuristic for valid approvals
        - approvals_after_last_push: Optional[int] - approvals after last commit
        """
        # Start with basic mapping
        pr_dict = self._map_graphql_pr(owner, repo, node)

        # Extract review data
        reviews_nodes = node.get("reviews", {}).get("nodes", [])
        latest_reviews_nodes = node.get("latestOpinionatedReviews", {}).get("nodes", [])

        # Extract approvers - unique reviewers who have at least one APPROVED review
        approvers_set = set()
        for review in reviews_nodes:
            if review.get("state") == "APPROVED":
                author = review.get("author", {})
                if author and author.get("login"):
                    approvers_set.add(author["login"].lower())

        approvers = sorted(list(approvers_set))
        approvers_count = len(approvers)

        # Build latest_approvals from latestOpinionatedReviews first, fallback to reviews
        latest_approvals = []
        if latest_reviews_nodes:
            # Use latestOpinionatedReviews (preferred)
            for review in latest_reviews_nodes:
                if review.get("state") == "APPROVED":
                    author = review.get("author", {})
                    if author and author.get("login"):
                        latest_approvals.append(
                            {
                                "login": author["login"],
                                "submitted_at": review.get("submittedAt"),
                            }
                        )
        else:
            # Fallback: find latest APPROVED review per author from all reviews
            reviewer_latest: Dict[str, Dict[str, Any]] = {}
            for review in reviews_nodes:
                if review.get("state") == "APPROVED":
                    author = review.get("author", {})
                    if author and author.get("login"):
                        login = author["login"]
                        submitted_at = review.get("submittedAt")
                        if submitted_at:
                            submitted_dt = self._parse_timestamp(submitted_at)
                            if submitted_dt:
                                if (
                                    login not in reviewer_latest
                                    or submitted_dt
                                    > reviewer_latest[login]["timestamp"]
                                ):
                                    reviewer_latest[login] = {
                                        "login": login,
                                        "submitted_at": submitted_at,
                                        "timestamp": submitted_dt,
                                    }

            # Convert to list (remove internal timestamp)
            latest_approvals = [
                {"login": data["login"], "submitted_at": data["submitted_at"]}
                for data in reviewer_latest.values()
            ]

        # Get review decision from GraphQL
        review_decision = node.get("reviewDecision")

        # Calculate heuristic metrics
        updated_at = node.get("updatedAt")
        merged_at = node.get("mergedAt")
        updated_dt = self._parse_timestamp(updated_at) if updated_at else None
        merged_dt = self._parse_timestamp(merged_at) if merged_at else None

        # approvals_valid_now: count approvals considered valid at query time
        approvals_valid_now = None
        if latest_approvals and (updated_dt or merged_dt):
            reference_time = merged_dt or updated_dt
            if reference_time:
                valid_count = 0
                for approval in latest_approvals:
                    approval_dt = self._parse_timestamp(approval["submitted_at"])
                    if approval_dt and approval_dt <= reference_time:
                        valid_count += 1
                approvals_valid_now = valid_count

        # approvals_after_last_push: approvals submitted after last commit (use updatedAt as proxy)
        approvals_after_last_push = None
        if latest_approvals and updated_dt:
            after_push_count = 0
            for approval in latest_approvals:
                approval_dt = self._parse_timestamp(approval["submitted_at"])
                if approval_dt and approval_dt > updated_dt:
                    after_push_count += 1
            approvals_after_last_push = after_push_count

        # Add new fields to PR dict
        pr_dict.update(
            {
                "approvers": approvers,
                "approvers_count": approvers_count,
                "latest_approvals": latest_approvals,
                "review_decision": review_decision,
                "approvals_valid_now": approvals_valid_now,
                "approvals_after_last_push": approvals_after_last_push,
            }
        )

        return pr_dict

    def apply_heuristic_review_rounds(self, prs: List[Dict[str, Any]]) -> None:
        """Apply a lightweight heuristic for review rounds on already mapped GraphQL PRs.

        Heuristic logic:
        - If commits > 1 and there is at least one review, assume at least 1 review round.
        - Additional rounds approximated by floor((commits - 1) / 2) bounded by reviews_count - 1.
        - synchronize_after_first_review approximated by max(commits - 1, 0).
        - re_review_pushes approximated by max(commits - reviews_count, 0).
        This intentionally over-simplifies to avoid REST timeline calls.
        """
        for pr in prs:
            try:
                if pr.get("source_api") != "graphql":
                    continue
                commits = pr.get("commits") or 0
                reviews = pr.get("reviews_count") or 0
                if not commits or not reviews:
                    pr["review_rounds"] = 0
                    pr["synchronize_after_first_review"] = 0
                    pr["re_review_pushes"] = 0
                    continue
                base_rounds = 1 if commits > 1 else 0
                extra = max((commits - 2) // 2, 0)
                max_possible = max(reviews - 1, 0)
                review_rounds = min(base_rounds + extra, max_possible)
                pr["review_rounds"] = review_rounds
                pr["synchronize_after_first_review"] = max(commits - 1, 0)
                pr["re_review_pushes"] = max(commits - reviews, 0)
            except Exception as e:
                self.logger.debug(
                    f"Heuristic review rounds failed for PR {pr.get('number')}: {e}"
                )

    def get_enriched_pull_requests_graphql(
        self,
        owner: str,
        repo: str,
        since_iso: Optional[str] = None,
        page_size: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get enriched pull requests using GraphQL - replaces multiple REST API calls.

        This method fetches all the data that would normally require:
        - 1 REST call for PRs list
        - 6 REST calls per PR for enrichment (details, reviews, comments, timeline, commits)

        Instead, it uses a single GraphQL query that gets all the data at once.

        Args:
            owner: Repository owner
            repo: Repository name
            since_iso: ISO timestamp to filter PRs updated since this date
            page_size: Number of PRs to fetch per page

        Returns:
            List of enriched PR dictionaries with all metrics included
        """
        self.logger.info(f"Fetching enriched PRs via GraphQL for {owner}/{repo}")

        try:
            # Use the enhanced GraphQL query that includes all the data we need
            prs = self.fetch_pull_requests_graphql(owner, repo, since_iso, page_size)

            # No automatic fallback - focus on fixing GraphQL
            if len(prs) == 0:
                self.logger.warning(
                    f"GraphQL returned 0 PRs for {owner}/{repo}. Check GraphQL query and authentication."
                )

            self.logger.info(
                f"GraphQL optimization: fetched {len(prs)} PRs with full enrichment "
                f"in {len(prs) // page_size + 1} API calls instead of {1 + len(prs) * 6} REST calls"
            )

            return prs

        except Exception as e:
            self.logger.error(f"Failed to fetch enriched PRs via GraphQL: {e}")
            raise e  # Re-raise to force fixing GraphQL instead of fallback

    def _quick_rest_pr_check(self, owner: str, repo: str) -> List[Dict[str, Any]]:
        """Quick REST call to check if repository has PRs (first page only)."""
        try:
            data = self._make_request(
                "GET",
                f"/repos/{owner}/{repo}/pulls",
                cache_key=f"quick_pr_check_{owner}_{repo}",
                cache_expiration=5,  # 5 minutes cache
                params={"state": "all", "per_page": 10, "page": 1},
            )
            if isinstance(data, list):
                return [d for d in data if isinstance(d, dict)]  # type: ignore[return-value]
            return []
        except Exception as e:
            self.logger.warning(f"Quick REST PR check failed: {e}")
            return []

    def _fallback_to_rest_enrichment(
        self,
        owner: str,
        repo: str,
        since_iso: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fallback method using the original REST API approach.
        This maintains compatibility when GraphQL fails.
        """
        self.logger.warning(
            "Using REST fallback - this will be slower due to multiple API calls"
        )

        # Get basic PR list
        prs = self.get_pull_requests(owner, repo, state="all")

        # Filter by date if provided
        if since_iso:
            cutoff = self._parse_timestamp(since_iso)
            if cutoff:
                filtered: List[Dict[str, Any]] = []
                for pr in prs:
                    upd = self._parse_timestamp(pr.get("updated_at", ""))
                    if upd and upd >= cutoff:
                        filtered.append(pr)
                prs = filtered

        # Enrich each PR with detailed data (this is the expensive part)
        enriched_prs = []
        for pr in prs:
            enriched_pr = self.enrich_pull_request(
                owner,
                repo,
                pr,
                include_size_metrics=True,
                include_review_metrics=True,
                include_review_rounds=True,
            )
            enriched_prs.append(enriched_pr)

        return enriched_prs
        return enriched_prs
