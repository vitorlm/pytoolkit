"""
GitHub API Client for PR analysis with rate limiting and caching.
"""

import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests

from utils.cache_manager.cache_manager import CacheManager
from utils.logging.logging_manager import LogManager


class GitHubApiClient:
    """GitHub REST API client with rate limiting and caching."""

    BASE_URL = "https://api.github.com"
    API_VERSION = "2022-11-28"

    def __init__(self, max_workers: int = 4):
        self.logger = LogManager.get_instance().get_logger("GitHubApiClient")
        self.cache = CacheManager.get_instance()
        self.max_workers = max_workers

        self.token = os.getenv("GITHUB_TOKEN")
        if not self.token:
            raise ValueError(
                "GitHub token is required. Set GITHUB_TOKEN environment variable."
            )

        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": self.API_VERSION,
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
            cached_data = self.cache.load(
                cache_key, expiration_minutes=cache_expiration
            )
            if cached_data is not None:
                self.logger.debug(f"Using cached data for {endpoint}")
                return cached_data

        url = urljoin(self.BASE_URL, endpoint.lstrip("/"))
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
                self.logger.debug(
                    f"Fetched page {page} from {endpoint}, got {len(data)} items"
                )

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
        all_members = set()

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
